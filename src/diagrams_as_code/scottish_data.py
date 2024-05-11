from diagrams import Diagram, Cluster
from diagrams.generic.blank import Blank

with Diagram("Record Type Definitions", show=True, direction="TB"):
    with Cluster("000 – Licensing and Information"):
        rt_000 = Blank("Detail A")
        
    with Cluster("001 - Activity"):
        rt_001 = Blank("Detail C")
        
    with Cluster("002 - Project for Activity"):
        rt_002 = Blank("Detail E")
        
    with Cluster("003 - Project"):
        rt_003 = Blank("Detail G")
    
    
