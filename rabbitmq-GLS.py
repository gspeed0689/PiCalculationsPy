import pika
import os, sys, argparse, json, uuid
from decimal import getcontext, Decimal

#set the number of decimal points the calculations will be run in, good as a global
getcontext().prec = 2**16

debug_lvl = 1
def bdb(lvl: int, msg: str):
    """Basic debug function that doesn't require importing anything
        Any message that is at the debug_lvl global or lower will be printed to the cmd line
        bdb = Basic DeBug

    Args:
        lvl (int): integer level of debugging
        msg (str): message to be printed
    """
    if lvl <= debug_lvl:
        print(msg)

def arguments():
    """command line arguments function for grabbing cmd line arguments with argparse
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endinteger", type=int,
                        help="An ending integer you wish your program to stop at, pick a big number (like more than a billion)")
    parser.add_argument("-s", "--stepsize", default=1000000, type=int,
                        help="Step size is the number of numbers to be calculated in a single go, computers are fast, pick a reasonably big number (like 1 million)")
    parser.add_argument("-p", "--processnumber", type=int,
                        help="Which process this program will run on. 0 = producer, 1>= calculator")
    args = parser.parse_args()
    
    bdb(1, "Arguments Parsed")
    bdb(2, f"-e: {args.endinteger}")
    bdb(2, f"-s: {args.stepsize}")
    bdb(2, f"-p: {args.processnumber}")
    return(args)

def work_producer(endNumber: int, blockStepping: int):
    """This function sends json strings to the GLS_ranges RabbitMQ queue created in main()
        The function needs an end number that acts as the denominator in the Gregory-Leibniz
            series. The function does not need a start number, the start number for the denominator
            is always 1. 
        The block stepping is the number of calculations a single run of a processor runs
            in a given range. 

    Args:
        endNumber (int): The end number for the denominator in the Gregory-Leibniz Series calcuations
        blockStepping (int): How many steps of the Gregory-Leibniz series should be given to a processor at a time
    """
    for i in range(1, endNumber, blockStepping):
        d = {"start":i, "end":i+blockStepping}
        bdb(2, f"work_producer: {json.dumps(d)}")
        channel.basic_publish(exchange='',
                      routing_key='GLS_ranges',
                      body=f'{json.dumps(d)}')

def publish_range_results(publish_value: str):
    """This function will take a str object generated from a decimal.Decimal object
        and publish it to the GLS_results RabbitMQ queue. 

    Args:
        publish_value (str): string of the decimal.Decimal object from the calculations function. 
    """
    channel.basic_publish(exchange='',
                      routing_key='GLS_results',
                      body=f'{str(publish_value)}')

def calculations(startingN: int, endingN: int):
    """Run the calculations on the integer range provided using the Gregory-Leibniz Series
        of calculating pi. 
        The function determines using the starting integer whether in the Gregory-Leibniz
            series the calculation should be added or subtracted to the current value.
        The function uses the Decimal library to be better than silly floats.
        The function keeps a running calculation for its range of calculations.
        The function calls the publish_range_results function to push the results to a 
            queue so another function can bring all of the calculations together. The 
            publish functions sends a string of the running calculation to the 
            publish_range_results function. 

    Args:
        startingN (int): input starting integer
        endingN (int): input ending integer
    """
    if type(startingN) != int:
        startingN = int(startingN)
        bdb(2, f"calculations: converted startingN to int")
    if type(endingN) != int:
        endingN = int(endingN)
        bdb(2, f"calculations: converted endingN to int")
    range_value = Decimal(0.0)
    if (startingN + 1)%4 == 0:
        current_operator = "-"
        bdb(2, f"calculations: starting operator -")
    elif (startingN + 1)%4 == 2:
        current_operator = "+"
        bdb(2, f"calculations: starting operator +")
    for i in range(startingN, endingN, 2):
        current_calculation = Decimal(4)/Decimal(i)
        if current_operator == "+":
            range_value += current_calculation
            current_operator = "-"
        elif current_operator == "-":
            range_value -= current_calculation
            current_operator = "+"
    bdb(2, f"calculations: {range_value}")
    publish_range_results(str(range_value))

def main():
    getcontext().prec = 2**16
    cmdargs = arguments()
    bdb(1, "main: received cmd args")
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    global channel
    channel = connection.channel()
    channel.queue_declare(queue='GLS_ranges', durable=True)
    channel.queue_declare(queue='GLS_results', durable=True)
    channel.basic_qos(prefetch_count=1)
    #Process 0: Produce Jobs
    if cmdargs.processnumber == 0:
        bdb(1, "producer")
        work_producer(cmdargs.endinteger, cmdargs.stepsize)
    #Process 1: Accumulate Results
    elif cmdargs.processnumber == 1:
        bdb(1, "accumulator")
        def callback_accumlation(ch, method, properties, body):
            if os.path.exists("calculation.txt") == False:
                f = open("calculation.txt", "w")
                f.write(f"{str(Decimal(0.0))}")
                f.close()
            with open("calculation.txt", "r") as pi_file:
                pi_calc = Decimal(pi_file.read())
            pi_calc += Decimal(body.decode("utf-8"))
            with open("calculation.txt", "w") as pi_file:
                pi_file.write(str(pi_calc))
            with open(f"returndata.{str(uuid.uuid4())}.txt","w") as f:
                f.write(body.decode("utf-8"))
        channel.basic_consume(queue='GLS_results',
                      auto_ack=True,
                      on_message_callback=callback_accumlation)
        channel.start_consuming()
    #process >=2: Run the series calculations
    elif cmdargs.processnumber >= 2:
        bdb(1, "worker")
        def callback_calculations(ch, method, properties, body):
            v = json.loads(body)
            bdb(2, f"callback: {v}")
            calculations(v['start'], v['end'])
        channel.basic_consume(queue='GLS_ranges',
                      auto_ack=True,
                      on_message_callback=callback_calculations)
        channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)