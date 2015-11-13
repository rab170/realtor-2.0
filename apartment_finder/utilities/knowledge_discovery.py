

import psycopg2
import numpy as np
import matplotlib.pyplot as plt

y_label = 'rent'
x_label = 'br'
pg_conn = psycopg2.connect('dbname=apartment_listings user=an0nym1ty')
cursor = pg_conn.cursor()

select = 'select {0}, {1}  from ACTIVE_LISTINGS where rent > 0 and ft2>0'.format(x_label, y_label)
cursor.execute(select)
result = cursor.fetchall()

x  = [ pair[0] for pair in result ]
y   = [ pair[1] for pair in result ]

plt.scatter(x, y)
plt.xlabel(x_label)
plt.ylabel(y_label)
plt.show()

