from gevent_zookeeper.framework import ZookeeperFramework
import os.path


def convert_egress(msg, me):
    items = []
    for k, v in msg.items():
        if isinstance(v, Address):
            v = {'_pyact_address': v.actor_id,
                 '_pyact_node': me}
        elif isinstance(v, RemoteAddress):
            v = {'_pyact_address': v.actor_id,
                 '_pyact_node': v.node}
        elif isinstance(v, dict):
            v = handle_address(v, me)
        items.append((k, v))
    return dict(items)

KEYS = ('_pyact_address', '_pyact_node')


def recv(receive, sock, address):
    buf = ''
    while True:
        data = sock.read(4)
        size = struct.unpack("!I", data)[0]
        data = data[4:]
        while len(data) < size:
            rest = size - len(data)
            more = sock.read(rest)
            data = ''.join([data, more])
        address | {'socket': 'framed', 'data': data}

def send(receive, sock):
    while True:
        pat, msg = receive()
        data = msg['data']
        sock.write(struct.pack("!I", len(data)) + data)


def generate_custom(obj, name):
    address = RemoteAddress.from_json(obj, name)
    if address:
        return address
    return obj


def handle_ingress(msg):
    items = []
    for k, v in msg.items():
        if isinstance(v, dict):
            if sorted(v.keys()) == KEYS:
                v = RemoteAddress(v['_pyact_address'],
                                  v['_pyact_node'])
            else:
                v = handle_ingress(v)
        items.append((k, v))
    return dict(items)


class Node(actor.Actor):
    """Actor for communicating with a remote node."""

    def main(self, name, host, port):
        self.name = name

        socket = actor.spawn_link(FramedSocket, host, port,
            self.address)
        pat, msg = self.receive()
        
        links = {}

        def _actor(id):
            return actor.Actor.all_actors.get(id)

        DATA = {'socket': 'framed'}
        LINK = {'link': str}   # link: address to: address
        CAST = {'cast': object}
        ADDR = {'address': object}
        EXIT = {'exit': object, 'address': object}
        EXC  = {'exception': object, 'address': object}

        while True:
            pat, msg = self.receive(DATA, LINK, MSG, EXIT)
            if pat is DATA:
                msg = json.loads(msg['data'])
                if is_shaped(msg, LINK):
                    _actor(msg['link']).add_link(
                        RemoteAddress(name, msg['to']))
                elif is_shaped(msg, CAST):
                    _actor(msg['cast'])._cast(msg['msg'],
                        as_json=False)
                elif is_shaped(msg, ADDR):
                    msg = json.dumps(msg, default=handle_custom)
                    msg['address'] = 
                    pass
                actor = actor.Actor.all_actors[msg['actor']]
                actor._cast(msg['msg'], as_json=False)
            elif pat is ADDR or pat is CAST or pat is LINK:
                data = json.dumps(handle_address(msg))
                socket.cast({'socket': 'framed', 'data': data})


class Node(object):
    """XXX

    @ivar links: Incoming links.  The key is.

    """

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.queue = Queue()
        self.links = {}

    def link(self, ractor_id, laddr):
        """."""
        lactor_id = laddr.actor_id
        self.queue.put(json.dumps({'link': ractor_id, 'to': lactor_id}))
        self.links[ractor_id] = 



class Address(object):
    """An Address is a reference to another Actor.  Any Actor which
    has an Address can asynchronously put a message in that Actor's
    mailbox. This is called a "cast". To send a message to another
    Actor and wait for a response, use "call" instead.
    """
    def __init__(self, actor):
        self.__actor = weakref.ref(actor)

    def to_json(self):
        return {'_pyact_address':self.actor_id}

    @classmethod
    def from_json(cls,obj):
        if obj.keys() == ['_pyact_address']:
            return Actor.all_actors[obj['_pyact_address']].address
        return None

    @staticmethod
    def lookup(name):
        """Return the Address of an Actor given the actor_id as a string.
        """
        return Actor.all_actors[name].address

    @property
    def _actor(self):
        """This will be inaccessible to Python code in the C implementation.
        """
        actor = self.__actor()
        if actor is None or actor.dead:
            raise DeadActor()
        return actor

    @property
    def actor_id(self):
        actor = self.__actor()
        if actor is None:
            raise DeadActor()
        return actor.actor_id

    def link(self, trap_exit=True):
        """Link the current Actor to the Actor at this address. If the linked
        Actor has an exception or exits, a message will be cast to the current
        Actor containing details about the exception or return result.
        """
        self._actor.add_link(curactor().address, trap_exit=trap_exit)

    def cast(self, message):
        """Send a message to the Actor this object addresses.
        """
        ## If messages are any Python objects (not necessarily dicts),
        ## but they specify the _as_json_obj() method, that method
        ## will be called to get  json object representation of that
        ## object.
        if hasattr(message,'_as_json_obj'):
            message = message._as_json_obj()
        self._actor._cast(json.dumps(message, default=handle_custom))

    def __or__(self, message):
        """Use Erlang-y syntax (| instead of !) to send messages.
               addr | msg
        is equivalent to:
               addr.cast(msg)
        """
        self.cast(message)

    def call(self, method, message=None, timeout=None):
        """Send a message to the Actor this object addresses.  Wait
        for a result. If a timeout in seconds is passed, raise
        C{gevent.Timeout} if no result is returned in less than the
        timeout.

        This could have nicer syntax somehow to make it look like an
        actual method call.
        """
        message_id = str(uuid.uuid1())
        my_address = curactor().address
        self.cast(
                {'call': message_id, 'method': method,
                'address': my_address, 'message': message})
        if timeout is None:
            cancel = None
        else:
            ## Raise any TimeoutError to the caller so they can handle it
            cancel = gevent.Timeout(timeout)
            cancel.start()

        RSP = {'response': message_id, 'message': object}
        EXC = {'response': message_id, 'exception': object}
        INV = {'response': message_id, 'invalid_method': str}

        pattern, response = curactor().receive(RSP, EXC, INV)

        if cancel is not None:
            cancel.cancel()

        if pattern is INV:
            raise RemoteAttributeError(method)
        elif pattern is EXC:
            raise RemoteException(response)
        return response['message']

    def __getattr__(self,method):
        """Support address.<method>(message,timout) call pattern.

        For example:
              addr.call('test') could be written as addr.test()

        """
        f = lambda message=None, timeout=None: self.call(
            method, message, timeout)
        return f

    def wait(self):
        """Wait for the Actor at this Address to finish, and return
        it's result.
        """
        return self._actor.greenlet.get()

    def kill(self):
        """Violently kill the Actor at this Address. Any other Actor which has
        called wait on this Address will get a Killed exception.
        """
        self._actor.greenlet.kill(Killed)




class RemoteAddress(object):
    """Representation of an address to a (possible) remote mailbox."""

    actor_id = property(lambda self: self._actor_id)
    node = property(lambda self: self._node)

    def __init__(self, actor_id, node):
        self._actor_id = actor_id
        self._node = node

    def cast(self, message):
        """Pass a message to the remote actor."""
        dispatcher.cast(self.node, self.actor_id,
            message)

    def link(self, trap_exit):
        dispatcher.link(self.node, self.actor_id,
            curactor().address)

    def kill(self):
        dispatcher.kill(self.node, self.actor_id,
            curactor().address)


class ZooKeeperDispatcher(MonitorListener):

    def __init__(self, name, framework):
        self.name = name
        self.framework = framework
        self.monitors = {}
        self.nodes = {}

    def created(self, name, data):
        """Attach a new node to the dispatcher."""
        if path == self.name:
            return

        self.nodes[name] = Node(*data.split(':'))
        self.notify_monitors('node-up', name, host, port)

    def deleted(self, node):
        """A node was deleted -- it is down."""
        self.nodes[node].close()
        self.notify_monitors('node-down', name)

    def notify_monitors(self, state, *args):
        """Notify actors that are monitor the dispatcher state."""
        msg = tuple([state]) + args
        for mref, monitor in self.monitors.items():
            try:
                 monitor | msg
            except DeadActor:
                del self.monitors[mref]

    def start(self):
        """Start the dispatcher by registering the node in the node
        registry.
        """
        self.framework.create().parents_if_needed().as_ephemeral().for_path(
            os.path.join('nodes', self.name))
        self.monitor = self.framework.monitor().children().using(
            self).for_path('nodes')

    def cast(self, node, actor_id, message):
        message = handle_egress(message, self.name)
        self.nodes[node].cast(actor_id, message)

    def link(self, node, actor_id, curraddr):
        self.nodes[node].link(actor_id, curraddr)

    def kill(self, node, actor_id, curraddr):
        self.nodes[node].kill(actor_id)


def bootstrap_node(name, zookeeper):
    """Bootstrap a node."""
    framework = ZookeeperFramework(zookeeper, chroot='/pyact')
    framework.connect()
    dispatcher = ZooKeeperDispatcher(name, framework)
    dispatcher.start()
    return dispatcher


