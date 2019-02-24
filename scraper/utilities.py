import math
import pandas as pd

def coordinate_distance(A, B):
    r_earth = 6371000
    deg = lambda rad: math.pi * rad / 180

    if type(A) == tuple:
        A = {name: A[i] for i, name in enumerate(['lat', 'lng'])}
    if type(B) == tuple:
        B = {name: B[i] for i, name in enumerate(['lat', 'lng'])}

    delta = {}
    for dim in ['lat', 'lng']:
        delta[dim] = deg(A[dim] - B[dim])

    a = deg(A['lat']) * deg(B['lat'])
    b = math.sin(delta['lat'] / 2) ** 2 + \
        a * math.sin(delta['lng'] / 2) ** 2
    c = 2 * math.atan2(b ** 0.5, (1 - b) ** 0.5)
    return r_earth * c
