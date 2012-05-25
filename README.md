# python-actors

This is some work on Donovan Preston's python-actors [1] so that it
works with gevent.

Since the dawn of concurrency research, there have been two camps:
shared everything, and shared nothing. Most modern applications use
threads for concurrency, a shared everything architecture.

Actors, however, use a shared nothing architecture where lightweight
processes communicate with each other using message passing. Actors
can change their state, create a new Actor, send a message to any
Actor it has the Address of, and wait for a specific kind of message
to arrive in it's mailbox.

[1] https://bitbucket.org/fzzzy/python-actors

# What Is an Actor?

* An actor is a process
* An actor can change it's own state
* An actor can create another actor and get it's address
* An actor can send a message to any addresses it knows
* An actor can wait for a specific message to arrive in it's mailbox

# Why Use Actors?

* Only an actor can change it's own state
* Each actor is a process, simplifying control flow
* Message passing is easy to distribute
* Most exceptional conditions occur when waiting for a message
* Isolates error handling code
* Makes it easier to build fault tolerant distributed systems

# How Are Actors Implemented in Python Actors?

* Use gevent and greenlet Green Threads to Implement Processes
* This doesn't provide real isolation
* But python doesn't provide private either

# Copy Messages As They Are Sent Between Actors

* This provides good enough isolation
* We can serialize/deserialize with simplejson to copy
* This also makes messages network safe
* Other more optimized implementations possible

# Problem: Imported Modules Leak State Between Actors

* Possibility: Keep a unique copy of sys.modules for every actor
* Possibility: Seal modules in wrapper object preventing modification
* Reality: Just write code that doesn't abuse global module state
