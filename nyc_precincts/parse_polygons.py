import shapefile
from shapely.geometry import shape
import pygeoif.geometry
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from fastkml import  kml
import xml.etree.cElementTree as ET

"""
xmlns = lambda dom_name : '{{http://www.opengis.net/kml/2.2}}{0}'.format(dom_name)

def get_coords(poly_dom):
    cords = list(poly.iter( xmlns('coordinates') ))
    if len(cords) != 1: return []
    cords = cords[0].text
    build_tuple = lambda cord_txt : tuple( float(c) for c in cord_txt.split(','))
    cords = [ build_tuple(cord) for cord in cords.split() ]
    return cords


root = ET.parse('precincts.kml')
precincts = []
for precinct in root.iter( xmlns('MultiGeometry') ):
    polygons =  precinct.findall( xmlns('Polygon') )
    poly_cords = [ get_coords(poly) for poly in polygons ]
    precincts.append(poly_cords)


for precinct in precincts:
    fig = plt.figure()
    ax = fig.gca()
    for poly in precinct:
        print poly
        print type (poly[0][0])
        p =PolygonPatch(poly)
        ax.add_patch()
    ax.axis('scaled')
    plt.show()
"""

with file('precincts.kml') as f:
    kml = kml.KML()
    kml.from_string(f.read())

nyc = kml.features().next()
for precinct in nyc.features():
    print precinct.name
    shape(precinct.geometry)
    polygons = [item for item in precinct.geometry if type(item) == pygeoif.geometry.Polygon]
