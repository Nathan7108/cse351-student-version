"""
Course: CSE 351, week 10
File: functions.py
Author: <your name>

Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04


Requesting a family from the server:
family_id = 6128784944
data = get_data_from_server('{TOP_API_URL}/family/{family_id}')

Example JSON returned from the server
{
    'id': 6128784944, 
    'husband_id': 2367673859,        # use with the Person API
    'wife_id': 2373686152,           # use with the Person API
    'children': [2380738417, 2185423094, 2192483455]    # use with the Person API
}

Requesting an individual from the server:
person_id = 2373686152
data = get_data_from_server('{TOP_API_URL}/person/{person_id}')

Example JSON returned from the server
{
    'id': 2373686152, 
    'name': 'Stella', 
    'birth': '9-3-1846', 
    'parent_id': 5428641880,   # use with the Family API
    'family_id': 6128784944    # use with the Family API
}


--------------------------------------------------------------------------------------
You will lose 10% if you don't detail your part 1 and part 2 code below

Describe how to speed up part 1

I use depth-first search with threads. For each family I get the family data, then use
threads to get all the people at the same time (husband, wife, kids). After adding them
to the tree, I recursively go to the parent families. Threading lets me make multiple API
calls at once which makes it way faster. I use a lock to keep the tree safe when adding
things.


Describe how to speed up part 2

I use breadth-first search with threads. I use a queue to go through families level by
level. For each family I get the data and use threads to get all the people at once.
Parent families go into the queue. Multiple threads work on families from the queue at
the same time. This processes all families at one level before moving to the next level.
Threading makes it much faster.


Extra (Optional) 10% Bonus to speed up part 3

Same as part 2 but I use a semaphore to only allow 5 threads at a time. This keeps the
server from having more than 5 active threads. I get the semaphore before API calls and
release it after, so it stays at 5 max.

"""
from common import *
import queue
import threading

# -----------------------------------------------------------------------------
def depth_fs_pedigree(family_id, tree):
    seen = set()
    lock = threading.Lock()
    
    def process_family(fam_id):
        if fam_id is None:
            return
        
        with lock:
            if fam_id in seen:
                return
            seen.add(fam_id)
        
        family_data = get_data_from_server(f'{TOP_API_URL}/family/{fam_id}')
        if family_data is None:
            return
        
        family = Family(family_data)
        with lock:
            tree.add_family(family)
        
        threads = []
        people = {}
        people_lock = threading.Lock()
        
        def get_person(person_id, name):
            if person_id is None:
                return
            person_data = get_data_from_server(f'{TOP_API_URL}/person/{person_id}')
            if person_data is not None:
                person = Person(person_data)
                with lock:
                    tree.add_person(person)
                if name:
                    with people_lock:
                        people[name] = person
        
        if family.get_husband():
            t = threading.Thread(target=get_person, args=(family.get_husband(), 'husband'))
            t.start()
            threads.append(t)
        
        if family.get_wife():
            t = threading.Thread(target=get_person, args=(family.get_wife(), 'wife'))
            t.start()
            threads.append(t)
        
        for child_id in family.get_children():
            t = threading.Thread(target=get_person, args=(child_id, None))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        parent_threads = []
        
        if 'husband' in people and people['husband'].get_parentid():
            t = threading.Thread(target=process_family, args=(people['husband'].get_parentid(),))
            t.start()
            parent_threads.append(t)
        
        if 'wife' in people and people['wife'].get_parentid():
            t = threading.Thread(target=process_family, args=(people['wife'].get_parentid(),))
            t.start()
            parent_threads.append(t)
        
        for t in parent_threads:
            t.join()
    
    process_family(family_id)

# -----------------------------------------------------------------------------
def breadth_fs_pedigree(family_id, tree):
    seen = set()
    lock = threading.Lock()
    q = queue.Queue()
    q.put(family_id)
    seen.add(family_id)
    
    def process_family(fam_id):
        family_data = get_data_from_server(f'{TOP_API_URL}/family/{fam_id}')
        if family_data is None:
            return []
        
        family = Family(family_data)
        with lock:
            tree.add_family(family)
        
        threads = []
        people = {}
        people_lock = threading.Lock()
        parents = []
        
        def get_person(person_id, name):
            if person_id is None:
                return
            person_data = get_data_from_server(f'{TOP_API_URL}/person/{person_id}')
            if person_data is not None:
                person = Person(person_data)
                with lock:
                    tree.add_person(person)
                if name:
                    with people_lock:
                        people[name] = person
        
        if family.get_husband():
            t = threading.Thread(target=get_person, args=(family.get_husband(), 'husband'))
            t.start()
            threads.append(t)
        
        if family.get_wife():
            t = threading.Thread(target=get_person, args=(family.get_wife(), 'wife'))
            t.start()
            threads.append(t)
        
        for child_id in family.get_children():
            t = threading.Thread(target=get_person, args=(child_id, None))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        if 'husband' in people and people['husband'].get_parentid():
            parents.append(people['husband'].get_parentid())
        
        if 'wife' in people and people['wife'].get_parentid():
            parents.append(people['wife'].get_parentid())
        
        return parents
    
    while not q.empty():
        threads = []
        level = []
        
        while not q.empty():
            level.append(q.get())
        
        for fam_id in level:
            def worker(fam_id):
                parent_ids = process_family(fam_id)
                for parent_id in parent_ids:
                    with lock:
                        if parent_id not in seen:
                            seen.add(parent_id)
                            q.put(parent_id)
            
            t = threading.Thread(target=worker, args=(fam_id,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()

# -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
    seen = set()
    lock = threading.Lock()
    sem = threading.Semaphore(5)
    q = queue.Queue()
    q.put(family_id)
    seen.add(family_id)
    
    def process_family(fam_id):
        sem.acquire()
        family_data = get_data_from_server(f'{TOP_API_URL}/family/{fam_id}')
        sem.release()
        
        if family_data is None:
            return []
        
        family = Family(family_data)
        with lock:
            tree.add_family(family)
        
        threads = []
        people = {}
        parents = []
        
        def get_person(person_id, name):
            if person_id is None:
                return
            sem.acquire()
            person_data = get_data_from_server(f'{TOP_API_URL}/person/{person_id}')
            sem.release()
            if person_data is not None:
                person = Person(person_data)
                with lock:
                    tree.add_person(person)
                if name:
                    people[name] = person
        
        if family.get_husband():
            t = threading.Thread(target=get_person, args=(family.get_husband(), 'husband'))
            t.start()
            threads.append(t)
        
        if family.get_wife():
            t = threading.Thread(target=get_person, args=(family.get_wife(), 'wife'))
            t.start()
            threads.append(t)
        
        for child_id in family.get_children():
            t = threading.Thread(target=get_person, args=(child_id, None))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        if 'husband' in people and people['husband'].get_parentid():
            parents.append(people['husband'].get_parentid())
        
        if 'wife' in people and people['wife'].get_parentid():
            parents.append(people['wife'].get_parentid())
        
        return parents
    
    while not q.empty():
        threads = []
        level = []
        
        while not q.empty():
            level.append(q.get())
        
        for fam_id in level:
            def worker(fam_id):
                parent_ids = process_family(fam_id)
                for parent_id in parent_ids:
                    with lock:
                        if parent_id not in seen:
                            seen.add(parent_id)
                            q.put(parent_id)
            
            t = threading.Thread(target=worker, args=(fam_id,))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()