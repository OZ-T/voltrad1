import numpy as np

b = np.random.standard_normal((4, 5))

np.sum(b)
np.mean(b)
np.std(b)

import matplotlib.pyplot as plt
plt.plot(np.cumsum(b))
plt.xlabel('x axis')
plt.ylabel('y axis')
plt.grid(True)
plt.show()


c = np.resize(b, 20)
plt.figure()
plt.subplot(211)
plt.plot(c, 'ro') # red dots
plt.grid(True)
plt.subplot(212)
plt.bar(range(len(c)), c)
plt.grid(True)
plt.show()