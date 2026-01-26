import os
import json
import glob
import collections

class MECCompilerResource:
    """
    资源感知型 MEC 编译器 (Resource-Aware MEC Compiler)
    
    核心功能:
    1. 计算每个节点的理论最早完成时间 (MEC)。
    2. 引入 "NFU Scoreboard" 机制，解决 Fusion 节点的资源冲突。
       如果两个 Fusion 节点在同一时刻 Ready，强制将其中一个推迟，
       确保生成的 MEC 反映了物理资源限制。
    """
    def __init__(self, dag_data):
        self.dag_data = dag_data
        self.nodes = {} 
        self.max_mec = 0
        
        # [关键] NFU 记分牌: 记录 NFU 下一次空闲的物理时刻
        # 初始为 0
        self.nfu_next_free_time = 0

    def run(self):
        # 1. 构建节点字典 & 初始化
        # input dag is a list of dicts
        for item in self.dag_data:
            nid = item['id']
            self.nodes[nid] = {
                'id': nid,
                'type': item['type'],       # NORMAL, PARTIAL, FUSION
                'parents': item['parents'], # list of parent_ids
                'level': item['level'],
                'mec': 0
            }

        # 2. 拓扑排序 (Topological Sort)
        # 简单的按 level 排序即可保证父节点先被处理 (因为生成的DAG保证 level 单调)
        # 如果 level 相同，顺序不重要，除非涉及资源争抢 (Fusion节点)
        sorted_nodes = sorted(self.nodes.values(), key=lambda x: x['level'])
        
        # 3. 遍历计算 MEC
        for node in sorted_nodes:
            node_type = node['type']
            
            if node_type == "FUSION":
                self._calc_fusion_node(node)
            else:
                # NORMAL 或 PARTIAL 都是在 PE 上跑
                self._calc_pe_node(node)
            
            # 更新全局最大 MEC
            if node['mec'] > self.max_mec:
                self.max_mec = node['mec']
                
        return self.nodes

    def _get_parent_mecs(self, node):
        """辅助函数: 获取所有父节点的 MEC"""
        p_mecs = []
        for pid in node['parents']:
            # 必须确保父节点已经在之前的轮次算过了
            if pid in self.nodes:
                p_mecs.append(self.nodes[pid]['mec'])
            else:
                # 理论上不应发生，除非 DAG 破损
                print(f"[Warn] Parent {pid} not found for {node['id']}")
                p_mecs.append(0)
        return p_mecs

    def _calc_pe_node(self, node):
        """
        计算 PE 任务 (NORMAL / PARTIAL) 的 MEC
        逻辑: 模拟单 PE 内的串行累加 (Serial Accumulation)
        """
        parent_mecs = self._get_parent_mecs(node)
        
        # 如果没有父节点 (Root node)
        if not parent_mecs:
            # 假设第 1 个周期完成
            node['mec'] = 1 
            return

        # 1. 贪心策略: 假设数据流驱动，先做先到的
        sorted_p_mecs = sorted(parent_mecs)
        
        current_time = 0
        
        # 2. 模拟 PE 处理过程
        for t_arrival in sorted_p_mecs:
            # 什么时候能开始处理这条边?
            # 必须等数据到达 (t_arrival) 且 上一条边算完 (current_time)
            start_time = max(t_arrival, current_time)
            
            # 耗时: 1 Cycle (MAC)
            finish_time = start_time + 1
            current_time = finish_time
            
        # 3. 收尾动作 (Update)
        if node['type'] == "NORMAL":
            # Normal 节点需要额外的 Update 周期 (+1)
            node['mec'] = current_time + 1
        else:
            # PARTIAL 节点只产出中间和，不需要 Update，算完边即结束
            node['mec'] = current_time

    def _calc_fusion_node(self, node):
        """
        计算 NFU 任务 (FUSION) 的 MEC
        逻辑: Data Ready + Resource Ready
        """
        parent_mecs = self._get_parent_mecs(node)
        
        # 1. Data Ready: 必须等所有 Partial 子节点都做完
        # (通常是 4 个，但也可能有少的情况)
        if not parent_mecs:
            data_ready_time = 0
        else:
            data_ready_time = max(parent_mecs)
            
        # 2. Resource Ready: 查看记分牌
        resource_ready_time = self.nfu_next_free_time
        
        # 3. 确定实际开始时间 (Start Execution Time)
        # 既要数据齐，又要机器空
        start_exec_time = max(data_ready_time, resource_ready_time)
        
        # 4. 计算完成时间
        # 占用 1 Cycle 计算 -> 此时 NFU 释放
        # 再加 1 Cycle 延迟 (Latency/WriteBack) -> 下游可见
        
        # 更新记分牌: NFU 在 "开始 + 1" 后就空闲了
        self.nfu_next_free_time = start_exec_time + 1
        
        # 节点的 MEC (对下游可见的时间) 是 "开始 + 1(Exec) + 1(Delay)"
        node['mec'] = start_exec_time + 2


def batch_run_mec_resource(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # 寻找 _split.json 结尾的文件
    json_files = glob.glob(os.path.join(input_dir, "*_split.json"))
    
    if not json_files:
        print(f"[Error] No split DAG files found in {input_dir}")
        return

    print(f"Starting Resource-Aware MEC Compilation on {len(json_files)} files...")
    print("-" * 60)

    for f_path in json_files:
        fname = os.path.basename(f_path).replace("_split.json", "")
        print(f"Compiling: {fname}")
        
        with open(f_path, 'r') as f:
            dag_data = json.load(f)
            
        compiler = MECCompilerResource(dag_data)
        nodes_result = compiler.run()
        
        # --- 导出结果 ---
        # 我们只需要保存 {node_id: mec_value} 的映射表供 Scheduler 使用
        mec_map = {}
        # 同时为了方便 debug，也可以按 LC 分组保存
        lc_schedule = collections.defaultdict(list)
        
        for nid, data in nodes_result.items():
            mec = data['mec']
            mec_map[str(nid)] = mec
            lc_schedule[mec].append(nid)
            
        # 1. 保存 MEC Map (JSON)
        out_path_map = os.path.join(output_dir, f"{fname}_mec_resource.json")
        with open(out_path_map, 'w') as f:
            json.dump(mec_map, f, indent=4)
            
        # 2. 保存易读的 Schedule (TXT) 用于人工检查
        out_path_txt = os.path.join(output_dir, f"{fname}_mec_debug.txt")
        with open(out_path_txt, 'w') as f:
            f.write(f"Resource-Aware MEC Report: {fname}\n")
            f.write(f"Max MEC: {compiler.max_mec}\n")
            f.write("-" * 80 + "\n")
            f.write("Format: LC [Time] (Count): Node_ID_List\n")
            f.write("-" * 80 + "\n")
            
            for lc in sorted(lc_schedule.keys()):
                nodes = sorted(lc_schedule[lc], key=str) # 转str排序保证一致性
                # 将该 LC 下所有节点 ID 拼接成字符串
                nodes_str = ", ".join(map(str, nodes))
                f.write(f"LC {lc:04d} ({len(nodes)} nodes): {nodes_str}\n")
                
        print(f"  -> Max MEC: {compiler.max_mec}")
        print(f"  -> Saved to: {out_path_map}")
        print("-" * 60)

if __name__ == "__main__":
    BASE_DIR = os.getcwd()
    # 输入: Split 后的 DAG 文件夹
    INPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "input_data_split")
    # 输出: MEC 结果文件夹 (我们新建一个 resource_mec 文件夹)
    OUTPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "output_data", "mec_resource")
    
    batch_run_mec_resource(INPUT_DIR, OUTPUT_DIR)