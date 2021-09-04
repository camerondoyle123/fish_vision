

class multiTools:
    """"
    some generic multi-threading tools so you can download stuff
    super speedy-zoom-zoom as well as send it to Mongo
    """

    # initialise
    def __init__(self, func):
        self.func = func # a method passed from the mongo_fish library

    @staticmethod
    def get_n_thread(n,t=list(range(1,20)),get_max=False):
        """
        n: int, number of pages (or ids) to cycle through total from find_last_api_page().
        t: list, range of numbers that reflect the optimal number of threads
        to select from. Should return numbers divisible by n.
        """
        
        l = []
        for i in t:
            if n % i == 0:
                l.append(i)
            
        if not l:
            print('unable to find divisible integer within range, exiting...')
        else:
            if get_max:
                l = max(l)
        return l



    def threaded_process_range(self, id_list):
        # this creates an n-length array to place your objects in
        nthreads = get_n_thread(len(id_list), get_max=True)

        threads = []

        # create the threads
        for i in range(nthreads):
            id_range = id_list[i::nthreads]
            t = Thread(target=self.func, args=([id_range]))
            threads.append(t)

        # start the threads
        [t.start() for t in threads]
        # wait for the threads to finish
        [t.join() for t in threads]
