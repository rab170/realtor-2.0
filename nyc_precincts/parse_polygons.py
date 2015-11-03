import xml.etree.cElementTree as ET

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
    print [len(poly) for poly in  precinct]

