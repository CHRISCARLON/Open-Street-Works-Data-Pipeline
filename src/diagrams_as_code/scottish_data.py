from diagrams import Diagram, Cluster
from diagrams.generic.blank import Blank

with Diagram("Scottish Road Works Register", show=True, direction="TB", outformat="png", filename="src/diagrams_as_code/scottish_road_works_data"):
    with Cluster("000 – Licensing and Information"):
        rt_000 = Blank("Text")
        
    with Cluster("001 - Activity"):
        rt_001 = Blank("Text")
        
    with Cluster("002 - Project for Activity"):
        rt_002 = Blank("Text")
        
    with Cluster("003 - Project"):
        rt_003 = Blank("Text")
    
    
