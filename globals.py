import queue
import threading

decision_queue = queue.Queue()
stop_flag = threading.Event()