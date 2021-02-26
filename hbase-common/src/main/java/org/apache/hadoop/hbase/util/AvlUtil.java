/**
 *
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

package org.apache.hadoop.hbase.util;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Helper class that allows to create and manipulate an AvlTree.
 * The main utility is in cases where over time we have a lot of add/remove of the same object,
 * and we want to avoid all the allocations/deallocations of the "node" objects that the
 * java containers will create.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class AvlUtil {
  private AvlUtil() {}

  /**
   * This class represent a node that will be used in an AvlTree.
   * Instead of creating another object for the tree node,
   * like the TreeMap and the other java contains, here the node can be extended
   * and the content can be embedded directly in the node itself.
   * This is useful in cases where over time we have a lot of add/remove of the same object.
   */
  @InterfaceAudience.Private
  public static abstract class AvlNode<TNode extends AvlNode> {
    protected TNode avlLeft;
    protected TNode avlRight;
    protected int avlHeight;

    public abstract int compareTo(TNode other);
  }

  /**
   * This class extends the AvlNode and adds two links that will be used in conjunction
   * with the AvlIterableList class.
   * This is useful in situations where your node must be in a map to have a quick lookup by key,
   * but it also require to be in something like a list/queue.
   * This is useful in cases where over time we have a lot of add/remove of the same object.
   */
  @InterfaceAudience.Private
  public static abstract class AvlLinkedNode<TNode extends AvlLinkedNode> extends AvlNode<TNode> {
    protected TNode iterNext = null;
    protected TNode iterPrev = null;
  }

  @InterfaceAudience.Private
  public interface AvlInsertOrReplace<TNode extends AvlNode> {
    TNode insert(Object searchKey);
    TNode replace(Object searchKey, TNode prevNode);
  }

  /**
   * The AvlTree allows to lookup an object using a custom key.
   * e.g. the java Map allows only to lookup by key using the Comparator
   * specified in the constructor.
   * In this case you can pass a specific comparator for every needs.
   */
  @InterfaceAudience.Private
  public static interface AvlKeyComparator<TNode extends AvlNode> {
    int compareKey(TNode node, Object key);
  }

  /**
   * Visitor that allows to traverse a set of AvlNodes.
   * If you don't like the callback style of the visitor you can always use the AvlTreeIterator.
   */
  @InterfaceAudience.Private
  public static interface AvlNodeVisitor<TNode extends AvlNode> {
    /**
     * @param node the node that we are currently visiting
     * @return false to stop the iteration. true to continue.
     */
    boolean visitNode(TNode node);
  }

  /**
   * Helper class that allows to create and manipulate an AVL Tree
   */
  @InterfaceAudience.Private
  public static class AvlTree {
    /**
     * @param root the current root of the tree
     * @param key the key for the node we are trying to find
     * @param keyComparator the comparator to use to match node and key
     * @return the node that matches the specified key or null in case of node not found.
     */
    public static <TNode extends AvlNode> TNode get(TNode root, final Object key,
        final AvlKeyComparator<TNode> keyComparator) {
      while (root != null) {
        int cmp = keyComparator.compareKey(root, key);
        if (cmp > 0) {
          root = (TNode)root.avlLeft;
        } else if (cmp < 0) {
          root = (TNode)root.avlRight;
        } else {
          return (TNode)root;
        }
      }
      return null;
    }

    /**
     * @param root the current root of the tree
     * @return the first (min) node of the tree
     */
    public static <TNode extends AvlNode> TNode getFirst(TNode root) {
      if (root != null) {
        while (root.avlLeft != null) {
          root = (TNode)root.avlLeft;
        }
      }
      return root;
    }

    /**
     * @param root the current root of the tree
     * @return the last (max) node of the tree
     */
    public static <TNode extends AvlNode> TNode getLast(TNode root) {
      if (root != null) {
        while (root.avlRight != null) {
          root = (TNode)root.avlRight;
        }
      }
      return root;
    }

    /**
     * Insert a node into the tree. It uses the AvlNode.compareTo() for ordering.
     * NOTE: The node must not be already in the tree.
     * @param root the current root of the tree
     * @param node the node to insert
     * @return the new root of the tree
     */
    public static <TNode extends AvlNode> TNode insert(TNode root, TNode node) {
      if (root == null) return node;

      int cmp = node.compareTo(root);
      assert cmp != 0 : "node already inserted: " + root;
      if (cmp < 0) {
        root.avlLeft = insert(root.avlLeft, node);
      } else {
        root.avlRight = insert(root.avlRight, node);
      }
      return balance(root);
    }

    /**
     * Insert a node into the tree.
     * This is useful when you want to create a new node or replace the content
     * depending if the node already exists or not.
     * Using AvlInsertOrReplace class you can return the node to add/replace.
     *
     * @param root the current root of the tree
     * @param key the key for the node we are trying to insert
     * @param keyComparator the comparator to use to match node and key
     * @param insertOrReplace the class to use to insert or replace the node
     * @return the new root of the tree
     */
    public static <TNode extends AvlNode> TNode insert(TNode root, Object key,
        final AvlKeyComparator<TNode> keyComparator,
        final AvlInsertOrReplace<TNode> insertOrReplace) {
      if (root == null) {
        return insertOrReplace.insert(key);
      }

      int cmp = keyComparator.compareKey(root, key);
      if (cmp < 0) {
        root.avlLeft = insert((TNode)root.avlLeft, key, keyComparator, insertOrReplace);
      } else if (cmp > 0) {
        root.avlRight = insert((TNode)root.avlRight, key, keyComparator, insertOrReplace);
      } else {
        TNode left = (TNode)root.avlLeft;
        TNode right = (TNode)root.avlRight;
        root = insertOrReplace.replace(key, root);
        root.avlLeft = left;
        root.avlRight = right;
        return root;
      }
      return balance(root);
    }

    private static <TNode extends AvlNode> TNode removeMin(TNode p) {
      if (p.avlLeft == null)
        return (TNode)p.avlRight;
      p.avlLeft = removeMin(p.avlLeft);
      return balance(p);
    }

    /**
     * Removes the node matching the specified key from the tree
     * @param root the current root of the tree
     * @param key the key for the node we are trying to find
     * @param keyComparator the comparator to use to match node and key
     * @return the new root of the tree
     */
    public static <TNode extends AvlNode> TNode remove(TNode root, Object key,
        final AvlKeyComparator<TNode> keyComparator) {
      return remove(root, key, keyComparator, null);
    }

    /**
     * Removes the node matching the specified key from the tree
     * @param root the current root of the tree
     * @param key the key for the node we are trying to find
     * @param keyComparator the comparator to use to match node and key
     * @param removed will be set to true if the node was found and removed, otherwise false
     * @return the new root of the tree
     */
    public static <TNode extends AvlNode> TNode remove(TNode root, Object key,
        final AvlKeyComparator<TNode> keyComparator, final AtomicBoolean removed) {
      if (root == null) return null;

      int cmp = keyComparator.compareKey(root, key);
      if (cmp == 0) {
        if (removed != null) removed.set(true);

        TNode q = (TNode)root.avlLeft;
        TNode r = (TNode)root.avlRight;
        if (r == null) return q;
        TNode min = getFirst(r);
        min.avlRight = removeMin(r);
        min.avlLeft = q;
        return balance(min);
      } else if (cmp > 0) {
        root.avlLeft = remove((TNode)root.avlLeft, key, keyComparator);
      } else /* if (cmp < 0) */ {
        root.avlRight = remove((TNode)root.avlRight, key, keyComparator);
      }
      return balance(root);
    }

    /**
     * Visit each node of the tree
     * @param root the current root of the tree
     * @param visitor the AvlNodeVisitor instance
     */
    public static <TNode extends AvlNode> void visit(final TNode root,
        final AvlNodeVisitor<TNode> visitor) {
      if (root == null) return;

      final AvlTreeIterator<TNode> iterator = new AvlTreeIterator<>(root);
      boolean visitNext = true;
      while (visitNext && iterator.hasNext()) {
        visitNext = visitor.visitNode(iterator.next());
      }
    }

    private static <TNode extends AvlNode> TNode balance(TNode p) {
      fixHeight(p);
      int balance = balanceFactor(p);
      if (balance == 2) {
        if (balanceFactor(p.avlRight) < 0) {
          p.avlRight = rotateRight(p.avlRight);
        }
        return rotateLeft(p);
      } else if (balance == -2) {
        if (balanceFactor(p.avlLeft) > 0) {
          p.avlLeft = rotateLeft(p.avlLeft);
        }
        return rotateRight(p);
      }
      return p;
    }

    private static <TNode extends AvlNode> TNode rotateRight(TNode p) {
      TNode q = (TNode)p.avlLeft;
      p.avlLeft = q.avlRight;
      q.avlRight = p;
      fixHeight(p);
      fixHeight(q);
      return q;
    }

    private static <TNode extends AvlNode> TNode rotateLeft(TNode q) {
      TNode p = (TNode)q.avlRight;
      q.avlRight = p.avlLeft;
      p.avlLeft = q;
      fixHeight(q);
      fixHeight(p);
      return p;
    }

    private static <TNode extends AvlNode> void fixHeight(TNode node) {
      final int heightLeft = height(node.avlLeft);
      final int heightRight = height(node.avlRight);
      node.avlHeight = 1 + Math.max(heightLeft, heightRight);
    }

    private static <TNode extends AvlNode> int height(TNode node) {
      return node != null ? node.avlHeight : 0;
    }

    private static <TNode extends AvlNode> int balanceFactor(TNode node) {
      return height(node.avlRight) - height(node.avlLeft);
    }
  }

  /**
   * Iterator for the AvlTree
   */
  @InterfaceAudience.Private
  public static class AvlTreeIterator<TNode extends AvlNode> implements Iterator<TNode> {
    private final Object[] stack = new Object[64];

    private TNode current = null;
    private int height = 0;

    public AvlTreeIterator() {
    }

    /**
     * Create the iterator starting from the first (min) node of the tree
     */
    public AvlTreeIterator(final TNode root) {
      seekFirst(root);
    }

    /**
     * Create the iterator starting from the specified key
     * @param root the current root of the tree
     * @param key the key for the node we are trying to find
     * @param keyComparator the comparator to use to match node and key
     */
    public AvlTreeIterator(final TNode root, final Object key,
        final AvlKeyComparator<TNode> keyComparator) {
      seekTo(root, key, keyComparator);
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public TNode next() {
      final TNode node = this.current;
      seekNext();
      return node;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    /**
     * Reset the iterator, and seeks to the first (min) node of the tree
     * @param root the current root of the tree
     */
    public void seekFirst(final TNode root) {
      current = root;
      height = 0;
      if (root != null) {
        while (current.avlLeft != null) {
          stack[height++] = current;
          current = (TNode)current.avlLeft;
        }
      }
    }

    /**
     * Reset the iterator, and seeks to the specified key
     * @param root the current root of the tree
     * @param key the key for the node we are trying to find
     * @param keyComparator the comparator to use to match node and key
     */
    public void seekTo(final TNode root, final Object key,
        final AvlKeyComparator<TNode> keyComparator) {
      current = null;
      height = 0;

      TNode node = root;
      while (node != null) {
        if (keyComparator.compareKey(node, key) >= 0) {
          if (node.avlLeft != null) {
            stack[height++] = node;
            node = (TNode)node.avlLeft;
          } else {
            current = node;
            return;
          }
        } else {
          if (node.avlRight != null) {
            stack[height++] = node;
            node = (TNode)node.avlRight;
          } else {
            if (height > 0) {
              TNode parent = (TNode)stack[--height];
              while (node == parent.avlRight) {
                if (height == 0) {
                  current = null;
                  return;
                }
                node = parent;
                parent = (TNode)stack[--height];
              }
              current = parent;
              return;
            }
            current = null;
            return;
          }
        }
      }
    }

    private void seekNext() {
      if (current == null) return;
      if (current.avlRight != null) {
        stack[height++] = current;
        current = (TNode)current.avlRight;
        while (current.avlLeft != null) {
          stack[height++] = current;
          current = (TNode)current.avlLeft;
        }
      } else {
        TNode node;
        do {
          if (height == 0) {
            current = null;
            return;
          }
          node = current;
          current = (TNode)stack[--height];
        } while (current.avlRight == node);
      }
    }
  }

  /**
   * Helper class that allows to create and manipulate a linked list of AvlLinkedNodes
   */
  @InterfaceAudience.Private
  public static class AvlIterableList {
    /**
     * @param node the current node
     * @return the successor of the current node
     */
    public static <TNode extends AvlLinkedNode> TNode readNext(TNode node) {
      return (TNode)node.iterNext;
    }

    /**
     * @param node the current node
     * @return the predecessor of the current node
     */
    public static <TNode extends AvlLinkedNode> TNode readPrev(TNode node) {
      return (TNode)node.iterPrev;
    }

    /**
     * @param head the head of the linked list
     * @param node the node to add to the front of the list
     * @return the new head of the list
     */
    public static <TNode extends AvlLinkedNode> TNode prepend(TNode head, TNode node) {
      assert !isLinked(node) : node + " is already linked";
      if (head != null) {
        TNode tail = (TNode)head.iterPrev;
        tail.iterNext = node;
        head.iterPrev = node;
        node.iterNext = head;
        node.iterPrev = tail;
      } else {
        node.iterNext = node;
        node.iterPrev = node;
      }
      return node;
    }

    /**
     * @param head the head of the linked list
     * @param node the node to add to the tail of the list
     * @return the new head of the list
     */
    public static <TNode extends AvlLinkedNode> TNode append(TNode head, TNode node) {
      assert !isLinked(node) : node + " is already linked";
      if (head != null) {
        TNode tail = (TNode)head.iterPrev;
        tail.iterNext = node;
        node.iterNext = head;
        node.iterPrev = tail;
        head.iterPrev = node;
        return head;
      }
      node.iterNext = node;
      node.iterPrev = node;
      return node;
    }

    /**
     * @param head the head of the current linked list
     * @param otherHead the head of the list to append to the current list
     * @return the new head of the current list
     */
    public static <TNode extends AvlLinkedNode> TNode appendList(TNode head, TNode otherHead) {
      if (head == null) return otherHead;
      if (otherHead == null) return head;

      TNode tail = (TNode)head.iterPrev;
      TNode otherTail = (TNode)otherHead.iterPrev;
      tail.iterNext = otherHead;
      otherHead.iterPrev = tail;
      otherTail.iterNext = head;
      head.iterPrev = otherTail;
      return head;
    }

    /**
     * @param head the head of the linked list
     * @param node the node to remove from the list
     * @return the new head of the list
     */
    public static <TNode extends AvlLinkedNode> TNode remove(TNode head, TNode node) {
      assert isLinked(node) : node + " is not linked";
      if (node != node.iterNext) {
        node.iterPrev.iterNext = node.iterNext;
        node.iterNext.iterPrev = node.iterPrev;
        head = (head == node) ? (TNode)node.iterNext : head;
      } else {
        head = null;
      }
      node.iterNext = null;
      node.iterPrev = null;
      return head;
    }

    /**
     * @param head the head of the linked list
     * @param base the node which we want to add the {@code node} before it
     * @param node the node which we want to add it before the {@code base} node
     */
    public static <TNode extends AvlLinkedNode> TNode prepend(TNode head, TNode base, TNode node) {
      assert !isLinked(node) : node + " is already linked";
      node.iterNext = base;
      node.iterPrev = base.iterPrev;
      base.iterPrev.iterNext = node;
      base.iterPrev = node;
      return head == base ? node : head;
    }

    /**
     * @param node the node to check
     * @return true if the node is linked to a list, false otherwise
     */
    public static <TNode extends AvlLinkedNode> boolean isLinked(TNode node) {
      return node.iterPrev != null && node.iterNext != null;
    }
  }
}
