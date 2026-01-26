import json
import os
import math
import glob

class GraphRewriter:
    def __init__(self, threshold=20, split_factor=4):
        self.threshold = threshold
        self.split_factor = split_factor # 必须是 4，对应 NFU 输入

    def process_file(self, input_path, output_dir):
        with open(input_path, 'r') as f:
            raw_dag = json.load(f)
            
        new_dag = []
        total_split_count = 0
        
        for node in raw_dag:
            # [简化] 直接读取统一格式 id 和 parents
            # 如果你的生成器还没更新，这里会报错，但既然你觉得冗余，说明数据源已统一
            nid = node['id']
            parents = node['parents']
            level = node.get('level', 0)
            
            # --- 判定逻辑 ---
            if len(parents) <= self.threshold:
                # [Case A] 普通节点 -> NORMAL
                new_dag.append({
                    "id": str(nid), # 统一转字符串 ID
                    "type": "NORMAL",
                    "parents": [str(p) for p in parents],
                    "level": level,
                    "cost": len(parents) + 1 # Update cost
                })
            else:
                # [Case B] 超级节点 -> 拆分
                total_split_count += 1
                
                # 1. 拆分父节点列表
                num_parents = len(parents)
                chunk_size = math.ceil(num_parents / self.split_factor)
                
                partial_node_ids = []
                
                for k in range(self.split_factor):
                    # 切片
                    start = k * chunk_size
                    end = min((k + 1) * chunk_size, num_parents)
                    chunk_parents = parents[start:end]
                    
                    if not chunk_parents:
                        continue 
                        
                    # 创建 Partial Node
                    p_id = f"P_{nid}_{k}"
                    partial_node_ids.append(p_id)
                    
                    new_dag.append({
                        "id": p_id,
                        "type": "PARTIAL",
                        "parents": [str(p) for p in chunk_parents],
                        "level": level, 
                        "cost": len(chunk_parents) 
                    })
                
                # 2. 创建 Fusion Node (复用原 ID)
                new_dag.append({
                    "id": str(nid),
                    "type": "FUSION",
                    "parents": partial_node_ids, 
                    "level": level + 1,
                    "cost": 2 
                })

        # 保存
        fname = os.path.basename(input_path).replace(".json", "")
        # 防止重复添加后缀 (如果文件名本来就是 xxx_dag.json)
        if fname.endswith("_dag"): 
            base_name = fname 
        else: 
            base_name = fname + "_dag"
            
        out_name = f"{base_name}_split.json"
        out_path = os.path.join(output_dir, out_name)
        
        with open(out_path, 'w') as f:
            json.dump(new_dag, f, indent=4)
            
        print(f"Rewriter Report for {fname}:")
        print(f"  -> Threshold: {self.threshold}")
        print(f"  -> Split Nodes: {total_split_count}")
        print(f"  -> Saved to: {out_path}")
        return out_path

if __name__ == "__main__":
    BASE_DIR = os.getcwd()
    INPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "input_data")
    OUTPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "input_data_split")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    rewriter = GraphRewriter(threshold=20)
    
    # [修改] 扫描目录下所有 _dag.json 文件 (不再限制 longtail)
    files = glob.glob(os.path.join(INPUT_DIR, "*_dag.json"))
    
    if not files:
        print(f"No DAG files found in {INPUT_DIR}")
    else:
        print(f"Found {len(files)} DAG files to process.")
        for f in files:
            # 排除掉可能误读的 split 文件 (虽然 glob pattern *_dag.json 应该能避开 *_split.json)
            if "split" in f: continue
            rewriter.process_file(f, OUTPUT_DIR)