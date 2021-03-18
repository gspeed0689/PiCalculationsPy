# PiCalculationsPy
**Practice code for calculating pi using Python, RabbitMQ, and IPythonNotebooks.**

Pi day with the other day, and I was interested in quickly writing different python scripts to implement the various ways of calculating pi. The basic methods were very easy to write, and are in the notebook `Different Ways of Calculating Pi.ipynb`.

This seemed really easy, and I wanted to take it a step further, so I decided to add a multiprocessing through a message passing intermediary aspect to calculating pi.
The script `rabbitmq-GLS.py` is my first use of a message queue and runs the Gregory-Leibniz series for calculating pi, and it makes use of a RabbitMQ server on the local machine.
`run_rabbitmq_GLS.py` is an easy script to spawn all of the different processing nodes for running the calculation. 
This script also uses the `decimal` library to have more decimal points in the calculation than the `float` type would provide. 
