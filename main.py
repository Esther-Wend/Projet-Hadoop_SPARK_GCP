from collections import defaultdict
 
class Graph:
    
    def __init__(self):
        self.graph = defaultdict(list)
 
    def add_edge(self, u, v):
        self.graph[u].append(v)
 
    def __dfs_util(self, v, visited):
        visited.add(v)
        print(v, end=' ')
        for neighbour in self.graph[v]:
            if neighbour not in visited:
                self.__dfs_util(neighbour, visited)

    def dfs(self, v):
        visited = set()
        self.__dfs_util(v, visited)
        
g = Graph()
g.add_edge(0, 1)
g.add_edge(0, 2)
g.add_edge(1, 2)
g.add_edge(2, 0)
g.add_edge(2, 3)
g.add_edge(3, 3)
 
print("DSF à partir du noeud 0 :")
g.dfs(0)