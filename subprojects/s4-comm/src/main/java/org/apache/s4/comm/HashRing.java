/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.s4.comm;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.s4.base.Hasher;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;


/**
 * A hash ring is used to map resources to a to a set of nodes.
 * This hash ring implements
 * <a href="http://www8.org/w8-papers/2a-webserver/caching/paper2.html#chash1">
 * Consistent Hashing</a> and therefore adding a removing nodes minimally
 * changes how resources are map to the nodes.  
 * <p/>
 * This implementation also allows you to apply non-uniform node weighting.  This
 * feature is usefull when you want to allocate more resources to some nodes and
 * fewer to others.
 * <p/>
 * The default weight of node is 200.  The weight of a node determins how many
 * points on the hash ring the node is alocated. Higher node weights increases
 * the uniform distribution of resources.
 * <p/>
 * Note that the order that nodes are added to the ring impact how resources
 * map to the nodes due to node hash collisions.
 *
 */
public class HashRing<Node, Resource> {
    private static final Logger logger = LoggerFactory.getLogger(HashRing.class);

    public static int DEFAULT_WEIGHT = 200;

    private static class Wrapper<N> {
        private N node;
        private int weight;

        public Wrapper(N node, int weight) {
            this.node = node;
            this.weight = weight;
        }

        @Override
        public String toString() {
            return "Wrapper{" + "node=" + node + ", weight=" + weight + '}';
        }
    }
    
    public long hash(String hashKey) {
    	byte[] digest = null;
    	try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(hashKey.getBytes("UTF-8"));
			digest = md.digest();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    	
        int b = 378551;
        int a = 63689;
        long hash = 0;

        for (int i = 0; i < digest.length; i++) {
            hash = hash * a + (int)digest[i];
            a = a * b;
        }

        return Math.abs(hash);
    }

    //@Inject 
    //private Hasher hasher;
    
    private final TreeMap<Integer, Wrapper<Node>> ring = new TreeMap<Integer, Wrapper<Node>>();
    private final LinkedHashMap<Node, Wrapper<Node>> nodes = new LinkedHashMap<Node, Wrapper<Node>>();

    /**
     * Constructs a <tt>HashRing</tt> which uses the OBJECT_HASHER to hash the nodes and values.
     *
     */
    public HashRing() {
    }


    /**
     * Adds all the specified nodes to the <tt>HashRing</tt> using the default
     * weight of 200 for each node.
     *
     * @param nodes the nodes to add
     */
    public void addAll(Iterable<Node> nodes) {
        for (Node node : nodes) {
            add(node);
        }
    }

    /**
     * Adds all the specified nodes to the <tt>HashRing</tt> using the default
     * weight of 200 for each node.
     *
     * @param nodes the nodes to add
     */
    public void add(Node... nodes) {
        addAll(Arrays.asList(nodes));
    }

    /**
     * Adds a node to the <tt>HashRing</tt> using the default
     * weight of 200 for the node.
     *
     * @param node the node to add
     */
    public void add(Node node) {
        add(node, DEFAULT_WEIGHT);
    }

    /**
     * Adds a node to the <tt>HashRing</tt> using the specified weight.
     *
     * @param node the node to add
     * @param weight the number of hash replicas to create the node in the <tt>HashRing</tt>
     * @throws IllegalArgumentException if the weight is less than 1
     */
    public void add(Node node, int weight) {
        if( weight < 1 ) {
            throw new IllegalArgumentException("weight must be 1 or greater");
        }

        Wrapper<Node> wrapper = new Wrapper<Node>(node, weight);
        logger.error("HashRing add node: " + node.toString());
        nodes.put(node, wrapper);
        for (int i = 0; i < wrapper.weight; i++) {
        	int index = (int) hash(node.toString() + i);
        	//logger.debug("lvl[" + i +"] index["+index+"] + str(" + node.toString() +")");
            ring.put((int)hash(node.toString() + i), wrapper);
        }
    }

    /**
     * Removes a previously added node from the <tt>HashRing</tt>
     *
     * @param node the node to remove
     * @return true if the node was previously added
     */
    public boolean remove(Node node) {
        Wrapper<Node> wrapper  = nodes.remove(node);
        if( wrapper == null ) {
            return false;
        }

        // We HAVE to re-hash the ring to keep it consistent since
        // nodes hashes may collide and last node added takes over the
        // the previously added node.  Order matters.
        ring.clear();
        for (Wrapper<Node> w : nodes.values()) {
            for (int i = 0; i < w.weight; i++) {
                ring.put((int)hash(w.node.toString() + i), w);
            }
        }
        return true;
    }

    /**
     * Removes all previously added nodes.
     */
    public void clear() {
        ring.clear();
        nodes.clear();
    }

    /**
     * @return all the previously added nodes.
     */
    public List<Node> getNodes() {
        return new ArrayList(nodes.keySet());
    }

    /**
     * Maps a resource value to a node.
     *
     * @param resource the resource to map
     * @return the Node that the resource maps to or null if the <tt>HashRing</tt> is empty.
     */
    public Node get(Resource resource) {
        Map.Entry<Integer, Wrapper<Node>> entry = getFirstEntry(resource);
        if (entry==null) {
            return null;
        }
        return entry.getValue().node;
    }

    /**
     * Maps a resource value to an interator to the nodes in the <tt>HashRing</tt>
     * starting at the Node which resource maps to.
     *
     * Note that duplicate node objects may be returned.  This is because
     *
     *
     *
     * @param resource the resource to map
     * @return a Iterator
     */
    public Iterator<Node> iterator(Resource resource) {
        final Map.Entry<Integer, Wrapper<Node>> first = getFirstEntry(resource);

        return new Iterator<Node>() {
            Map.Entry<Integer, Wrapper<Node>> removealCandidate;
            Map.Entry<Integer, Wrapper<Node>> last;
            Map.Entry<Integer, Wrapper<Node>> next = first;

            public boolean hasNext() {
                // We might allready know the next entry..
                if( next != null )
                    return true;

                // Since we use last to figure out the next..
                if( last==null )
                    return false;

                // Figure out the enxt entry...
                Map.Entry<Integer, Wrapper<Node>> next = ring.higherEntry(last.getKey());
                if( next == null ) {
                    next = ring.firstEntry();
                }

                // We don't need last anymore..
                last = null;

                // But the next entry might circle back to the first...
                if( next.getKey()==first.getKey() ) {
                    next = null;
                }
                return next!=null;
            }

            public Node next() {
                if( !hasNext() ) {
                    throw new NoSuchElementException();
                }
                removealCandidate = last = next;
                next = null;
                return last.getValue().node;
            }

            public void remove() {
                if( removealCandidate ==null ) {
                    throw new IllegalStateException();
                }
                HashRing.this.remove(last.getValue().node);
                removealCandidate =null;
            }
        };
    }

    private Map.Entry<Integer, Wrapper<Node>> getFirstEntry(Resource resource) {
        if (ring.isEmpty()) {
            return null;
        }
        int hash = (int) hash(resource.toString());
        logger.debug(String.format("Key[%s] hash[%d]", resource.toString(), hash));
        Map.Entry<Integer, Wrapper<Node>> entry = ring.ceilingEntry(hash);
        if( entry == null ) {
            entry = ring.firstEntry();
        }
        return entry;
    }


}