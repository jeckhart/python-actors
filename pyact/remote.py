# Copyright (c) 2012 Johan Rydberg
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from gevent_zookeeper.framework import ZookeeperFramework
from gevent.server import StreamServer

from pyact import actor


class RemoteNode(object):
    """XXX"""

    def __init__(self, mesh, id, cookie):
        self.mesh = mesh
        self.id = id
        self.cookie = cookie

    def _cast(self, address, message):
        """For internal use.

        Send a message to an actor on this node.
        """
        _actor = self.actors.get(address.actor_id)
        if _actor is None or _actor.dead:
            # Silently drop the message.
            return
        _actor._cast(message)

from gevent import socket
import os.path
import msgpack


class ZookeeperCoordinator(object):
    """Network coordinator that discovers nodes using zookeeper."""

    def __init__(self, mesh, localnode, coordinator, address=None, port=45429):
        self.framework = ZookeeperFramework(coordinator, chroot='/pyact')
        self.localnode = localnode
        self.mesh = mesh
        self.port = port
        self.address = address

    def _publish(self):
        """Publish the local node."""
        self.framework.create().parents_if_needed().as_ephemeral().with_data(
            '%s:%d' % (self.address, self.port)).for_path(
            os.path.join('nodes', self.localnode.id))

    def accept(self, socket, address):
        """Accept an incoming connection.

        Note that this is called in an isolated greenlet, we can
        therefor block.
        """
        unpacker = msgpack.Unpacker()
        while True:
            try:
                data = socket.read()
                if not data:
                    break
                unpacker.feed(data)
            except socket.error:
                break
            else:
                for message in unpacker.unpack():
                    if message[0] == 2:
                        msgtype, method, params = message
                        if method == 'cast':
                            self.mesh.cast(actor.Address.from_json(
                                params[0]), params[1])

    def start(self):
        """Start the coordinator.

        This will connect to the ZooKeeper cluster and register our
        local node.  It will also establish connections to other
        parties of the mesh.
        """
        self.server = StreamServer(('0.0.0.0', self.port), self.accept)
        self.server.start()
        self.framework.connect()

