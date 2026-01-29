import os
import json
import random
import matplotlib.pyplot as plt
import numpy as np

## 生成 Long-Tail 矩阵的 DAG

class LongTailDAGGenerator:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def generate(self, dim=100, super_node_ratio=0.10, visualize=True):
        print(f"Generating Long-Tail Matrix: Dim={dim}, SuperNode Ratio={super_node_ratio:.0%}")
        
        adjacency = {i: [] for i in range(dim)}
        levels = {i: 0 for i in range(dim)}
        viz_rows, viz_cols = [], [] # For visualization
        
        # 定义超级行的索引 (随机选择)
        num_super = int(dim * super_node_ratio)
        # 避开前 10 行，因为前面节点太少，没法做超级节点
        possible_indices = list(range(10, dim))
        super_indices = set(random.sample(possible_indices, min(num_super, len(possible_indices))))
        
        total_edges = 0
        
        for i in range(dim):
            # A. 确定当前节点的入度目标
            if i in super_indices:
                # 超级节点：入度 20 ~ 40 (确保触发拆分阈值 20)
                target_degree = random.randint(20, 40)
            else:
                # 普通节点：入度 0 ~ 10 (稀疏)
                target_degree = random.randint(0, 10)
            
            # 修正：不能超过目前已有的节点数 (i)
            target_degree = min(target_degree, i)
            
            # B. 随机选择父节点
            if target_degree > 0:
                parents = random.sample(range(i), target_degree)
                parents.sort()
                adjacency[i] = parents
                total_edges += len(parents)
                
                # 计算 Level
                max_p_level = -1
                for p in parents:
                    if levels[p] > max_p_level: max_p_level = levels[p]
                    # 记录可视化坐标
                    viz_rows.append(i)
                    viz_cols.append(p)
                levels[i] = max_p_level + 1
            else:
                levels[i] = 0
            
            # 对角线可视化
            viz_rows.append(i)
            viz_cols.append(i)

        # C. 导出 JSON
        dag_list = []
        for i in range(dim):
            dag_list.append({
                "id": i,
                "parents": adjacency[i],
                "level": levels[i],
                # 预先标记，方便 debug，实际上由 Rewriter 决定类型
                "is_super": i in super_indices 
            })
            
        filename = f"longtail_{dim}_{int(super_node_ratio*100)}pct"
        save_path = os.path.join(self.output_dir, f"{filename}_dag.json")
        with open(save_path, 'w') as f:
            json.dump(dag_list, f, indent=4)
            
        print(f"  -> Generated {total_edges} edges. Saved to {save_path}")
        
        if visualize:
            self._save_spy_plot(filename, dim, viz_rows, viz_cols)
            
        return save_path

    def _save_spy_plot(self, name, dim, rows, cols):
        plt.figure(figsize=(6, 6))
        plt.scatter(cols, rows, c='blue', s=1, marker='s')
        plt.xlim(-1, dim)
        plt.ylim(-1, dim)
        plt.gca().invert_yaxis()
        plt.title(f"Sparsity: {name}")
        plt.savefig(os.path.join(self.output_dir, f"{name}_spy.png"), dpi=150)
        plt.close()

if __name__ == "__main__":
    # 生成目录
    BASE_DIR = os.getcwd()
    INPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "input_data")
    
    gen = LongTailDAGGenerator(INPUT_DIR)
    # 生成一个 100x100，含 10% 超级节点的矩阵
    gen.generate(dim=100, super_node_ratio=0.10)