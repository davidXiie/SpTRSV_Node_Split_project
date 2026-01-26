import os
import json
import collections
import time
import glob
import csv

# ==========================================
# Part 1: Compiler (MEC Calculation)
# ==========================================
class MECCompilerSimple:
    def __init__(self, dag_data):
        self.dag_data = dag_data
        self.nodes = {} 
        self.max_mec = 0

    def run(self):
        # 初始化
        for item in self.dag_data:
            # [修改] 适配新格式 id / parents
            nid = item.get('id', item.get('row_index')) # 兼容写法
            parents = item.get('parents', item.get('dependency_nodes'))
            
            self.nodes[nid] = {
                'id': nid,
                'parents': parents,
                'level': item['level'],
                'mec': 0
            }
        
        # 拓扑排序 (按 Level)
        sorted_ids = sorted(self.nodes.keys(), key=lambda k: self.nodes[k]['level'])
        
        # 计算 MEC
        for node_id in sorted_ids:
            self._calc_mec(self.nodes[node_id])
            
        return self.nodes

    def _calc_mec(self, node):
        parents = node['parents']
        if not parents:
            node['mec'] = 1
            if node['mec'] > self.max_mec: self.max_mec = node['mec']
            return

        parent_mecs = collections.defaultdict(int)
        for p_id in parents:
            p_mec = self.nodes[p_id]['mec']
            parent_mecs[p_mec] += 1

        sorted_times = sorted(parent_mecs.keys())
        current_avail_time = 0
        
        for t in sorted_times:
            count = parent_mecs[t]
            ls = max(t + 1, current_avail_time + 1)
            finish_time = ls + count - 1
            current_avail_time = finish_time
        
        node['mec'] = current_avail_time + 1
        if node['mec'] > self.max_mec: 
            self.max_mec = node['mec']

# ==========================================
# Part 2: Scheduler (Physical Simulation)
# ==========================================
class PhysicalSchedulerModeA:
    def __init__(self, dag_data, mec_map, pe_limit):
        self.pe_limit = pe_limit
        self.mec_map = mec_map
        self.dag_data = dag_data
        
        self.adj_list = collections.defaultdict(list)
        self.node_remaining_edges = {}
        
        self._build_graph()
        
        self.current_lc = 0
        self.pc = 0
        self.finished_nodes = set()
        self.nodes_finished_last_lc = []
        
        self.edge_task_list = [] 
        self.mandatory_queue = []
        
        self.trace_log = []
        self.total_ops = 0

    def _build_graph(self):
        for item in self.dag_data:
            # [修改] 适配新格式
            target_id = item.get('id', item.get('row_index'))
            parents = item.get('parents', item.get('dependency_nodes'))
            
            self.node_remaining_edges[target_id] = len(parents)
            
            # 构建反向图
            for p in parents:
                self.adj_list[p].append(target_id)

    def _calc_slack(self, target_id):
        target_mec = self.mec_map.get(str(target_id), 99999)
        rem_edges = self.node_remaining_edges[target_id]
        return target_mec - self.current_lc - rem_edges

    def run(self):
        total_nodes = len(self.dag_data)
        max_lc_limit = max(self.mec_map.values()) * 5 if self.mec_map else 2000
        
        while len(self.finished_nodes) < total_nodes:
            if self.current_lc > max_lc_limit:
                print(f"  [Warning] Timeout: LC exceeded {max_lc_limit}")
                break
                
            self.current_lc += 1
            nodes_finished_this_lc = []
            
            # 1. 释放边任务 (From Prev LC) [保持原逻辑]
            for src_id in self.nodes_finished_last_lc:
                children = self.adj_list[src_id]
                for child_id in children:
                    self.edge_task_list.append({'type': 'edge', 'src': src_id, 'target': child_id})
            self.nodes_finished_last_lc = [] 

            # 2. 加载 Mandatory (Update Tasks)
            for item in self.dag_data:
                nid = item.get('id', item.get('row_index'))
                if nid in self.finished_nodes: continue
                
                target_mec = self.mec_map.get(str(nid), 0)
                # 如果 MEC 到了 且 所有边做完了 (Ready)
                if target_mec <= self.current_lc and self.node_remaining_edges[nid] == 0:
                    is_in_queue = any(op['type'] == 'update' and op['target'] == nid for op in self.mandatory_queue)
                    if not is_in_queue:
                        self.mandatory_queue.append({'type': 'update', 'target': nid, 'slack': -999})

            # 3. Slack 抢占 (Promotion)
            tasks_by_target = collections.defaultdict(list)
            for task in self.edge_task_list: tasks_by_target[task['target']].append(task)
            
            to_remove_from_optional = []
            for tid, tasks in tasks_by_target.items():
                slack = self._calc_slack(tid)
                for t in tasks: t['slack'] = slack
                if slack <= 0 and tasks:
                    # 严格按照 scheduler3 的逻辑: 每个 LC 提升一个
                    chosen_task = tasks[0]
                    self.mandatory_queue.append(chosen_task)
                    to_remove_from_optional.append(chosen_task)

            if to_remove_from_optional:
                remove_set = {id(t) for t in to_remove_from_optional}
                self.edge_task_list = [t for t in self.edge_task_list if id(t) not in remove_set]

            # 4. PC 循环
            loop_active = True
            
            # 如果 Mandatory 为空，但还有 Update 任务刚刚 Ready，或者刚进入 LC，至少跑一轮检查
            # 为了完全对齐 Mode A，这里只要 Mandatory 非空就跑
            # 如果初始 Mandatory 为空，直接进入下一 LC (除非有 bug)
            # 在 scheduler3 中是 while loop_active (default True)，内部判空退出
            
            while loop_active:
                self.pc += 1
                self.edge_task_list.sort(key=lambda x: (x['slack'], x['target']))
                
                dispatched_ops = []
                busy_targets = set()
                pe_remaining = self.pe_limit
                
                # A. Mandatory
                next_mandatory = []
                for op in self.mandatory_queue:
                    if pe_remaining > 0 and op['target'] not in busy_targets:
                        pe_remaining -= 1
                        busy_targets.add(op['target'])
                        dispatched_ops.append(op)
                    else:
                        next_mandatory.append(op)
                self.mandatory_queue = next_mandatory
                
                # B. Optional
                next_edge = []
                for op in self.edge_task_list:
                    if pe_remaining > 0 and op['target'] not in busy_targets:
                        pe_remaining -= 1
                        busy_targets.add(op['target'])
                        dispatched_ops.append(op)
                    else:
                        next_edge.append(op)
                self.edge_task_list = next_edge

                # C. Update State
                op_log_strs = []
                for op in dispatched_ops:
                    tgt = op['target']
                    self.total_ops += 1
                    if op['type'] == 'update':
                        self.finished_nodes.add(tgt)
                        nodes_finished_this_lc.append(tgt)
                        op_log_strs.append(f"U({tgt})") 
                    else:
                        self.node_remaining_edges[tgt] -= 1
                        op_log_strs.append(f"E({op['src']}->{tgt})")

                self.trace_log.append({
                    'pc': self.pc,
                    'lc': self.current_lc,
                    'ops': op_log_strs
                })
                
                if not self.mandatory_queue:
                    loop_active = False
            
            self.nodes_finished_last_lc.extend(nodes_finished_this_lc)
        
        return self.pc, self.trace_log

def run_experiment(input_dir, output_dir, pe_count=64):
    if not os.path.exists(input_dir): return
    os.makedirs(output_dir, exist_ok=True)
    json_files = glob.glob(os.path.join(input_dir, "*_dag.json"))
    
    print(f"Running Baseline Experiment (PE={pe_count}) on {len(json_files)} files...")
    
    summary_results = []

    for f_path in json_files:
        filename = os.path.basename(f_path).replace("_dag.json", "")
        # 排除 split 后产生的 _split.json (如果混在一起的话)
        if "split" in filename: continue
            
        print(f"Processing: {filename}")
        
        with open(f_path, 'r') as f: dag_data = json.load(f)
            
        # Compile
        compiler = MECCompilerSimple(dag_data)
        nodes_result = compiler.run()
        max_mec = compiler.max_mec
        
        mec_map = {}
        for nid, data in nodes_result.items():
            mec_map[str(nid)] = data['mec']
            
        # Schedule
        scheduler = PhysicalSchedulerModeA(dag_data, mec_map, pe_limit=pe_count)
        total_cycles, trace_log = scheduler.run()
        
        # Save Trace
        trace_path = os.path.join(output_dir, f"{filename}_trace.txt")
        with open(trace_path, 'w') as f:
            f.write(f"Baseline Trace: {filename} (PE={pe_count})\n")
            f.write(f"MEC: {max_mec}, Cycles: {total_cycles}\n")
            f.write("-" * 60 + "\n")
            for entry in trace_log:
                ops_str = " ".join(entry['ops'])
                f.write(f"LC {entry['lc']:<4} | PC {entry['pc']:<4} | {ops_str}\n")
                
        summary_results.append({
            'Matrix': filename,
            'MEC': max_mec,
            'Cycles': total_cycles
        })

    # Summary
    if summary_results:
        csv_path = os.path.join(output_dir, "baseline_summary.csv")
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Matrix', 'MEC', 'Cycles'])
            writer.writeheader()
            writer.writerows(summary_results)
        print(f"Baseline Summary saved to: {csv_path}")

if __name__ == "__main__":
    base_dir = os.getcwd()
    input_dir = os.path.join(base_dir, "SpTRSV_Node_Split_project", "input_data")
    output_dir = os.path.join(base_dir, "SpTRSV_Node_Split_project", "output_data", "baseline")
    run_experiment(input_dir, output_dir, pe_count=10)
