from diagrams import Diagram, Cluster
from diagrams.generic.blank import Blank

with Diagram("Street Works Simplified", show=True, direction="TB", outformat="png", filename="src/diagrams_as_code/street_works_simplified", node_attr={'fontname': 'Helvetica', 'fontsize': '12', 'fontcolor': 'black'}):
    with Cluster("Periphery", graph_attr={'bgcolor': 'white', 'style': 'dashed', 'color': 'black', 'penwidth': '1', 'fontsize': '14'}):
        EVs = Blank("Scaffolding, Skips, etc")
        Heat_Networks = Blank("Events\n(e.g. Marathon)")
        with Cluster("Dependent & New Infrastructure", graph_attr={'bgcolor': 'white', 'style': 'dashed', 'color': 'black', 'penwidth': '1', 'fontsize': '14'}):
            EVs = Blank("EV Charge Points")
            Heat_Networks = Blank("Heat Networks")
            with Cluster("Core Highways", graph_attr={'bgcolor': 'white', 'style': 'dashed', 'color': 'black', 'penwidth': '1', 'fontsize': '14'}):
                highway_utilities = [Blank("Highway Improvements"), Blank("Resurfacing")]
                with Cluster("Internet & Telecoms", graph_attr={'bgcolor': 'white', 'style': 'dashed', 'color': 'black', 'penwidth': '1', 'fontsize': '14'}):
                    internet_telecoms = Blank("Internet/Telecoms")
                    with Cluster("Traditional Utilities", graph_attr={'bgcolor': 'white', 'style': 'dashed', 'color': 'black', 'penwidth': '1', 'fontsize': '14'}):
                        core_utilities = [Blank("Gas"), Blank("Water"), Blank("Electricity")]

