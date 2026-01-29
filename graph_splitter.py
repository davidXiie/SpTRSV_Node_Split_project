import json
import os
import math
import glob

class GraphRewriter:
    """
    Graph Rewriter (Fixed-Size Splitting)
    功能: 将入度超过阈值的节点拆分为多个固定大小的 Partial 节点。
    例如: chunk_size=5, 入度12 -> 拆分为 [5, 5, 2] 三个 Partial 节点。
    """
    def __init__(self, threshold=5, chunk_size=5):
        self.threshold = threshold
        self.chunk_size = chunk_size 

    def process_file(self, input_path, output_dir):
        with open(input_path, 'r') as f:
            raw_dag = json.load(f)
            
        new_dag = []
        total_split_count = 0
        
        for node in raw_dag:
            # 兼容性读取
            nid = node.get('id', node.get('row_index'))
            parents = node.get('parents', node.get('dependency_nodes'))
            level = node.get('level', 0)
            
            # --- 判定逻辑 ---
            if len(parents) <= self.threshold:
                # [Case A] 普通节点 -> NORMAL
                new_dag.append({
                    "id": str(nid),
                    "type": "NORMAL",
                    "parents": [str(p) for p in parents],
                    "level": level,
                    "cost": len(parents) + 1 # Update cost
                })
            else:
                # [Case B] 大节点 -> 按固定大小切分
                total_split_count += 1
                num_parents = len(parents)
                
                partial_node_ids = []
                
                # 步进循环: 0, 5, 10, ...
                chunk_idx = 0
                for i in range(0, num_parents, self.chunk_size):
                    # 切片: 取 [i, i+5]
                    chunk_parents = parents[i : i + self.chunk_size]
                    
                    # 创建 Partial Node
                    p_id = f"P_{nid}_{chunk_idx}"
                    partial_node_ids.append(p_id)
                    
                    new_dag.append({
                        "id": p_id,
                        "type": "PARTIAL",
                        "parents": [str(p) for p in chunk_parents],
                        # Partial 节点的 level 保持与原节点一致(或略低)，具体由 MEC 重新计算决定
                        "level": level, 
                        "cost": len(chunk_parents) 
                    })
                    
                    chunk_idx += 1
                
                # 创建 Fusion Node (复用原 ID)
                # 注意: 现在的 Fusion 节点可能有 >4 个父节点 (如果原节点特别大)
                new_dag.append({
                    "id": str(nid),
                    "type": "FUSION",
                    "parents": partial_node_ids, # 依赖所有生成的 Partial
                    "level": level + 1,
                    "cost": 2 # 假设 Fusion 依然很快，或者你可以根据 partial_node_ids 长度动态调整
                })

        # 保存
        fname = os.path.basename(input_path).replace(".json", "")
        if fname.endswith("_dag"): 
            base_name = fname 
        else: 
            base_name = fname + "_dag"
            
        out_name = f"{base_name}_split.json"
        out_path = os.path.join(output_dir, out_name)
        
        with open(out_path, 'w') as f:
            json.dump(new_dag, f, indent=4)
            
        print(f"Rewriter Report for {fname}:")
        print(f"  -> Strategy: Fixed Chunk Size = {self.chunk_size}")
        print(f"  -> Split Nodes: {total_split_count}")
        print(f"  -> Total Nodes (After Split): {len(new_dag)}")
        print(f"  -> Saved to: {out_path}")
        return out_path

if __name__ == "__main__":
    BASE_DIR = os.getcwd()
    INPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "input_data")
    OUTPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "input_data_split")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 这里设置阈值和块大小都为 5
    rewriter = GraphRewriter(threshold=5, chunk_size=5)
    
    files = glob.glob(os.path.join(INPUT_DIR, "*_dag.json"))
    
    if not files:
        print(f"No DAG files found in {INPUT_DIR}")
    else:
        print(f"Found {len(files)} DAG files to process.")
        for f in files:
            if "split" in f: continue
            rewriter.process_file(f, OUTPUT_DIR)