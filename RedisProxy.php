<?php

/**
 * Created by silentium
 * Date: 01.09.12
 * Time: 16:20
 */


/**
 * @method bool set(string $key, mixed $value) Set the string value in argument as value of the key
 * @method string|bool get(string $key)  Gets a value stored at key. If the key doesn't exist, FALSE is returned
 * @method bool setex(string $key, int $ttl, mixed $value) Set the string value in argument as value of the key, with a time to live
 * @method bool setnx(string $key, mixed $value) Set the string value in argument as value of the key if the key doesn't already exist in the database
 * @method int del(string $key) Remove specified keys
 * @method int delete(string $key) Remove specified keys (alias for del)
 * @-method RedisProxy multi(int $mode = Redis::MULTI) Enter transactional mode
 * @-method void exec() Executes a transaction
 * @-method void discard() Cancels a transaction
 * @method void watch(string $key) Watches a key for modifications by another client. If the key is modified between WATCH and EXEC, the MULTI/EXEC transaction will fail (return FALSE)
 * @-method void unwatch() Cancels all the watching of all keys by this client
 * @-method void subscribe(string $channels, string $callback) Subscribe to channels.
 * @-method void publish(string $channels, string $message) Publish messages to channels.
 * @method bool exists(string $key) Verify if the specified key exists
 * @method int incr(string $key) Increment the number stored at key by one. If the key does not exist it's value is initialized to be 0 first
 * @method int incrBy(string $key, int $value) Increment the number stored at key by the specified value. If the key does not exist it's value is initialized to be 0 first
 * @method int decr(string $key) Decrement the number stored at key by one. If the key does not exist it's value is initialized to be 0 first
 * @method int decrBy(string $key, int $value) Decrement the number stored at key by the specified value. If the key does not exist it's value is initialized to be 0 first
 * @method array getMultiple(array $keys) Get the values of all the specified keys. If one or more keys dont exist, the array will contain FALSE at the position of the key
 * @method int|bool lPush(string $key, mixed $value) Adds the string value to the head (left) of the list. Creates the list if the key didn't exist. If the key exists and is not a list, FALSE is returned
 * @method int|bool rPush(string $key, mixed $value) Adds the string value to the tail (right) of the list. Creates the list if the key didn't exist. If the key exists and is not a list, FALSE is returned
 * @method int|bool lPushx(string $key, mixed $value) Adds the string value to the head (left) of the list if the list exists
 * @method int|bool ePushx(string $key, mixed $value) Adds the string value to the tail (right) of the list if the list exists
 * @method string|bool lPop(string $key) Return and remove the first element of the list
 * @method string|bool rPop(string $key) Return and remove the last element of the list
 * @method array blPop(string $keys, int $timeout) Is a blocking lPop primitive. If at least one of the lists contains at least one element, the element will be popped from the head of the list and returned to the caller. If all the list identified by the keys passed in arguments are empty, blPop will block during the specified timeout until an element is pushed to one of those lists. This element will be popped
 * @method array brPop(string $keys, int $timeout) Is a blocking rPop primitive. If at least one of the lists contains at least one element, the element will be
 * @method int|bool lSize(string $key) Returns the size of a list identified by Key. If the list didn't exist or is empty, the command returns 0. If
 * the data type identified by Key is not a list, the command return FALSE
 * @method string|bool lIndex(string $key, int $index) Return the specified element of the list stored at the specified key. 0 the first element, 1 the second ... -1
 * the last element, -2 the penultimate ... Return FALSE in case of a bad index or a key that doesn't point to a
 * list
 * @method string|bool lGet(string $key, int $index) Return the specified element of the list stored at the specified key. 0 the first element, 1 the second ... -1
 * the last element, -2 the penultimate ... Return FALSE in case of a bad index or a key that doesn't point to a
 * list (alias of lIndex)
 * @method bool lSet(string $key, int $index, mixed $value) Set the list at index with the new value
 * @method array lRange(string $key, int $start, int $end) Returns the specified elements of the list stored at the specified key in the range [start, end]. start and
 * stop are interpreted as indices: 0 the first element, 1 the second ... -1 the last element, -2 the
 * penultimate ...
 * @method array lGetRange(string $key, int $start, int $end) Returns the specified elements of the list stored at the specified key in the range [start, end]. start and
 * stop are interpreted as indices: 0 the first element, 1 the second ... -1 the last element, -2 the
 * penultimate ... (alias of lRange)
 * @method bool lTrim(string $key, int $start, int $end) Trims an existing list so that it will contain only a specified range of elements
 * @method bool listTrim(string $key, int $start, int $end) Trims an existing list so that it will contain only a specified range of elements (alias of lTrim)
 * @method int|bool lRem(string $key, mixed $value, int $count) Removes the first count occurences of the value element from the list. If count is zero, all the matching
 * elements are removed. If count is negative, elements are removed from tail to head
 * @method int|bool lRemove(string $key, mixed $value, int $count) Removes the first count occurences of the value element from the list. If count is zero, all the matching
 * elements are removed. If count is negative, elements are removed from tail to head (alias of lRem)
 * @method int lInsert(string $key, string $position, string $pivot, mixed $value) Insert value in the list before or after the pivot value. the parameter options specify the position of the
 * insert (before or after). If the list didn't exists, or the pivot didn't exists, the value is not inserted
 * @method bool sAdd(string $key, mixed $value) Adds a value to the set value stored at key. If this value is already in the set, FALSE is returned
 * @method bool sRem(string $key, string $member) Removes the specified member from the set value stored at key
 * @method bool sRemove(string $key, string $member) Removes the specified member from the set value stored at key (alias for sRem)
 * @method bool sMove(string $srcKey, string $dstKey, string $member) Moves the specified member from the set at srcKey to the set at dstKey
 * @method bool sIsMember(string $key, mixed $value) Checks if value is a member of the set stored at the key key
 * @method bool sContains(string $key, mixed $value) Checks if value is a member of the set stored at the key key (alias for sIsMember)
 * @method int sCard(string $key) Returns the cardinality of the set identified by key
 * @method int sSize(string $key) Returns the cardinality of the set identified by key (alias for sCard)
 * @method string|bool sPop(string $key) Removes and returns a random element from the set value at key
 * @method string|bool sRandMember(string $key) Returns a random element from the set value at Key, without removing it
 * @-method array|bool sInter(string $key1, string $key2) Returns the members of a set resulting from the intersection of all the sets held at the specified keys. If
 * just a single key is specified, then this command produces the members of this set. If one of the keys is
 * missing, FALSE is returned
 * @-method int|bool sInterStore($dstKey, string $key1, string $key2) Performs a sInter command and stores the result in a new set
 * @-method array sUnion(string $key1, string $key2) Performs the union between N sets and returns it
 * @-method array sUnionStore(string $dstKey, string $key1, string $key2) Performs the same action as sUnion, but stores the result in the first key
 * @-method array sDiff(string $key1, string $key2) Performs the difference between N sets and returns it
 * @-method array sDiffStore($dstKey, string $key1, string $key2) Performs the same action as sDiff, but stores the result in the first key
 * @method array sMembers(string $key) Returns the contents of a set
 * @method array sGetMembers(string $key) Returns the contents of a set (alias for sMembers)
 * @method string getSet(string $key, mixed $value) Sets a value and returns the previous entry at that key
 * @-method string randomKey() Returns a random key
 * @-method bool select(int $dbIndex) Switches to a given database
 * @-method bool move(string $key, int $dbIndex) Moves a key to a different database
 * @-method bool rename(string $srcKey, string $dstKey) Renames a key
 * @-method bool renameNx(string $srcKey, $string dstKey) Same as rename, but will not replace a key if the destination already exists. This is the same behaviour as
 * setNx
 * @method bool setTimeout(string $key, int $ttl) Sets an expiration date (a timeout) on an item
 * @method bool expire(string $key, int $ttl) Sets an expiration date (a timeout) on an item (alias for setTimeout)
 * @method bool expireAt(string $key, int $timestamp) Sets an expiration date (a timestamp) on an item
 * @-method array keys(string $pattern) Returns the keys that match a certain pattern
 * @-method array getKeys(string $pattern) Returns the keys that match a certain pattern (alias for keys)
 * @-method int dbSize() Return the current database's size
 * @-method bool auth(string $password) Authenticate the connection using a password. Warning: The password is sent in plain-text over the network
 * @-method bool bgrewriteaof() Starts the background rewrite of AOF (Append-Only File)
 * @-method bool slaveof(string $host=null, int $port=6379) Change the slave status for the current host
 * @-method string|int|bool object($info, $key) Describes the object pointed to by a key ????
 * @-method bool save() Performs a synchronous save
 * @-method bool bgsave() Performs a background save
 * @-method int lastSave() Returns the timestamp of the last disk save
 * @method int type(string $key) Returns the type of data pointed by a given key
 * @method int append(string $key, string $value) Append specified string to the string stored in specified key
 * @method string getRange(string $key, int $start, int $end) Return a substring of a larger string
 * @method int setRange(string $key, int $offset, string $value) Changes a substring of a larger string
 * @method void strlen(string $key) Get the length of a string value
 * @method int getBit(string $key, int $offset) Return a single bit out of a larger string
 * @method int setBit(string $key, int $offset, int $value) Changes a single bit of a string
 * @method array ttl(string $key) Returns the time to live left for a given key, in seconds. If the key doesn't exist, FALSE is returned
 * @method array sort(string $key, array $options = array()) Sort a set and return the sorted members
 * @method bool persist(string $key) Remove the expiration timer from a key
 * @method bool mset(array $pairs) Sets multiple key-value pairs in one atomic command
 * @method bool msetnx(array $pairs) Sets multiple key-value pairs in one atomic command, setting only keys that did not exist
 * @method string|bool rpoplpush(string $srcKey, string $dstKey) Pops a value from the tail of a list, and pushes it to the front of another list. Also return this value
 * @method string|bool brpoplpush(string $srcKey, string $dstKey, int $timeout = 0.0) A blocking version of rpoplpush, with an integral timeout in the third parameter
 * @method int zAdd(string $key, float $score, mixed $value) Adds the specified member with a given score to the sorted set stored at key
 * @method array zRange(string $key, float $start, float $end, bool $withScores = false) Returns a range of elements from the ordered set stored at the specified key, with values in the range
 * [start, end]. start and stop are interpreted as zero-based indices: 0 the first element, 1 the second ...
 * -1 the last element, -2 the penultimate ...
 * @method int zDelete(string $key, string $member) Deletes a specified member from the ordered set
 * @method int zRem(string $key, string $member) Deletes a specified member from the ordered set (alias of zDelete)
 * @method array zRevRange(string $key, int $start, int $end, bool $withScores = false) Returns the elements of the sorted set stored at the specified key in the range [start, end] in reverse order.
 * start and stop are interpretated as zero-based indices: 0 the first element, 1 the second ... -1 the last
 * element, -2 the penultimate ...
 * @method int|bool zCount(string $key, float $start, float $end) Returns the number of elements of the sorted set stored at the specified key which have scores in the range
 * [start,end]. Adding a parenthesis before start or end excludes it from the range. +inf and -inf are also valid
 * limits
 * @method array zRangeByScore(string $key, float $start, float $end, $options = array()) Returns the elements of the sorted set stored at the specified key which have scores in the range [start, end].
 * Adding a parenthesis before start or end excludes it from the range. +inf and -inf are also valid limits
 *
 * @method int zRemRangeByScore(string $key, float $start, float $end) Deletes the elements of the sorted set stored at the specified key which have scores in the range [start, end]
 * @method int zDeleteRangeByScore(string $key, float $start, float $end) Deletes the elements of the sorted set stored at the specified key which have scores in the range [start, end]
 * (alias for zRemRangeByScore)
 * @method int|array zRemRangeByRank(string $key, float $start, float $end, array $options = array()) Deletes the elements of the sorted set stored at the specified key which have rank in the range [start, end]
 * @method int|array zDeleteRangeByRank(string $key, float $start, float $end, array $options = array()) Deletes the elements of the sorted set stored at the specified key which have rank in the range [start, end] (alias of zRemRangeByRank)
 * @method int|array zSize(string $key) Returns the cardinality of an ordered set
 * @method int zCard(string $key) Returns the cardinality of an ordered set (alias for zSize)
 * @method float zScore(string $key, string $member) Returns the score of a given member in the specified sorted set
 * @method float zRank(string $key, string $member) Returns the rank of a given member in the specified sorted set, starting at 0 for the item with the smallest score.
 * @method float zRevRank(string $key, string $member) Returns the rank of a given member in the specified sorted set in reverse order
 * @method float zIncrBy(string $key, mixed $value, string $member) Increments the score of a member from a sorted set by a given amount
 * @-method int zUnion(string $keyOutput, array $zSetKeys, array $weights = array(), string $function) Creates an union of sorted sets given in second argument. The result of the union will be stored in the sorted
 * @-method int zInter(string $keyOutput, array $zSetKeys, array $weights = array(), string $function) Creates an intersection of sorted sets given in second argument. The result of the union will be stored in the
 * sorted set defined by the first argument. The third optionnel argument defines weights to apply to the sorted
 * sets in input. In this case, the weights will be multiplied by the score of each element in the sorted set
 * before applying the aggregation. The forth argument defines the AGGREGATE option which specify how the results
 * of the union are aggregated
 * @method int|bool hSet(string $key, string $hashKey, mixed $value) Adds a value to the hash stored at key. If this value is already in the hash, FALSE is returned
 * @method bool hSetNx(string $key, string $hashKey, mixed $value) Adds a value to the hash stored at key only if this field isn't already in the hash
 * @method string|bool hGet(string $key, string $hashKey) Gets a value from the hash stored at key. If the hash table doesn't exist, or the key doesn't exist, FALSE is
 * returned
 * @method int|bool hLen(string $key) Returns the length of a hash, in number of items
 * @method bool hDel(string $key, string $hashKey) Removes a value from the hash stored at key. If the hash table doesn't exist, or the key doesn't exist, FALSE
 * is returned
 * @method array hKeys(string $key) Returns the keys in a hash, as an array of strings
 * @method array hVals(string $key) Returns the values in a hash, as an array of strings
 * @method array hGetAll(string $key) Returns the whole hash, as an array of strings indexed by strings
 * @method bool hExists(string $key, string $memberKey) Verify if the specified member exists in a key
 * @method int hIncrBy(string $key, string $member, mixed $value) Increments the value of a member from a hash by a given amount
 * @method bool hMset(string $key, array $members) Fills in a whole hash. Non-string values are converted to string, using the standard (string)cast. NULL
 * values are stored as empty strings
 * @method array hMget(string $key, array $memberKeys) Retrieve the values associated to the specified fields in the hash

 */
class RedisProxy {

	// object pool for sources implementation
	protected static $_pool = array();


	/**
	 * @static
	 * @return RedisProxy
	 */
	public static function getInstance($index=null) {
		if (is_null($index)) {
			return reset(self::$_pool);
		}
		return self::$_pool[$index];
	}


	/**
	 * get number of data sources in the pool
	 *
	 * @static
	 * @return int
	 */
	public static function getInstanceCount() {
		return count(self::$_pool);
	}

	public static function erase() {
		foreach (self::$_pool as $obj) {
			$obj->close();
		}
		self::$_pool = array();
	}


	protected $_index;
	public $namespace = '';

	/**
	 * @var Redis[]
	 */
	protected $_connections = array();
	protected $_weights = array();


	protected $_replicas = 256;
	protected $_slicesCount = 0;
	protected $_slicesHalf = 0;
	protected $_slicesDiv = 0;

	protected $_hashring = array();
	protected $_hashringCount = 0;
	protected $_hashringIsInitialized = false;

	protected $_cache = array();
	protected $_cacheCount = 0;
	protected $_cacheMax = 256;

	protected $_is64 = false;

	public function __construct() {
		self::$_pool[] = $this;
		$this->_index = count(self::$_pool) - 1;
		$this->_is64 = PHP_INT_SIZE == 8;
	}

	public function __destruct() {
		$this->close();
		unset(self::$_pool[$this->_index]);
	}


	/**
	 * get index of this connection in the pool
	 *
	 * @return int
	 */
	public function getIndex() {
		return $this->_index;
	}


	/**
	 * Connect to server and add it to source
	 * $weight is used to determine relative amount of stored keys
	 *
	 * @param string $host
	 * @param int $port
	 * @param int $weight
	 */
	public function addServer($host = '127.0.0.1', $port = 6379, $dbIndex=0, $weight = 1) {
		$redis = new Redis();
		$connection_string = $host . ':' . $port . '/' . $dbIndex;
		if ($redis->pconnect($host, $port, .0, $connection_string)) {
			$redis->select($dbIndex);
			$this->_connections[$connection_string] = $redis;
			$this->_weights[$connection_string] = $weight;
			$this->_hashringIsInitialized = false;
			return true;
		}
		unset($redis);
		return false;
	}


	/**
	 * Create $_hashring map
	 */
	protected function _initializeHashring() {

		$connections_count = count($this->_connections);
		if ($connections_count < 2) {
			$this->_hashring = array();
			$this->_hashringCount = 0;

			$this->_slicesCount = 0;
			$this->_slicesHalf = 0;
			$this->_slicesDiv = 0;

		} else {

			$this->_slicesCount = ($this->_replicas * $connections_count) / 8;
			$this->_slicesHalf = $this->_slicesCount / 2;
			$this->_slicesDiv = (2147483648 / $this->_slicesHalf);

			// Initialize the hashring.
			$this->_hashring = array_fill(0, $this->_slicesCount, array());

			// Calculate the average weight.
			$avg = round(array_sum($this->_weights) / $connections_count, 2);

			// Interate over the backends.
			foreach ($this->_connections as $connection_string => &$connection) {
				// Adjust the weight.
				$weight = $this->_weights[$connection_string];
				$weight = round(($weight / $avg) * $this->_replicas);

				// Create as many replicas as $weight.
				for ($i = 0; $i < $weight; $i++) {
					$position = crc32($connection_string . ':' . $i);
					$slice = floor($position / $this->_slicesDiv);
					if (!$this->_is64 && $slice < 0) $slice += $this->_slicesCount; // 32bit workaround
					$this->_hashring[$slice][$position] =& $connection;
				}
			}

			// Sort each slice of the hashring.
			foreach ($this->_hashring as &$a) {
				ksort($a, SORT_NUMERIC);
			}
			ksort($this->_hashring, SORT_NUMERIC);

		}
		$this->_hashringIsInitialized = true;

		$this->_cleanCache();

	}


	/**
	 * returns connection resource pointing to server
	 * where key $name should be stored
	 *
	 * @param $name
	 * @return Redis
	 */
	public function getConnectionByKeyName($name) {
		// If we have only one backend, return it.
		if (count($this->_connections) == 1) {
			return reset($this->_connections);
		}

		if (!$this->_hashringIsInitialized) {
			$this->_initializeHashring();
		}

		// If the key has already been mapped, return the cached entry.
		if ($this->_cacheMax > 0 && isset($this->_cache[$name])) {
			return $this->_cache[$name];
		}

		$return = null;
		$crc32 = crc32($name);

		// Select the slice to begin with.
		$slice = floor($crc32 / $this->_slicesDiv);
		if (!$this->_is64 && $slice < 0) $slice += $this->_slicesCount; // 32bit workaround

		// walk through slice
		foreach ($this->_hashring[$slice] as $position => $connection) {
			if ($position < $crc32) continue;
			$return = $connection;
			break;
		}

		// Not found corresponding position. MUST be next one (first position of next slice)
		if (empty($return)) {
			// step next in circle
			$slice = ($slice + 1) % $this->_slicesCount;
			// get first position
			$return = reset($this->_hashring[$slice]);
		}

		// Cache the result for quick retrieval in the future.
		if ($this->_cacheMax > 0) {
//			 Add to internal cache.
			$this->_cache[$name] = $return;
			$this->_cacheCount++;

//			 If the cache is getting too big, clear it.
			if ($this->_cacheCount > $this->_cacheMax) {
				$this->_cleanCache();
			}
		}

		// Return the result.

		return $return;
	}


	/**
	 * Cleans cache
	 */
	protected function _cleanCache() {
		$this->_cache = array();
		$this->_cacheCount = 0;
	}

	public function __call($method, $args) {

		if (!method_exists('Redis', $method)) {
			throw new Exception("Method '$method' does not exist for phpredis");
		}
		if (!isset($args[0]) || !is_string($args[0])) {
			throw new Exception("Only methods with \$key as first parameter can be overloaded");
		}
		if (empty($this->_connections)) {
			return false;
		}

		if ($this->namespace) $args[0] = $this->namespace . $args[0];
		$redis = $this->getConnectionByKeyName($args[0]);

		return call_user_func_array(array($redis, $method), $args);
	}


	/**
	 * Check for active connections
	 *
	 * @return bool
	 */
	public function status() {
		return !empty($this->_connections);
	}

	/**
	 * Select database by $dbIndex
	 *
	 * @param $dbIndex
	 * @return bool
	 */
	public function select($dbIndex) {
		if (empty($this->_connections)) return false;
		$res = true;
		foreach ($this->_connections as $conn) {
			if (!$conn->select($dbIndex)) $res = false;
		}
		return $res;

	}


	/**
	 * close connections
	 */
	public function close() {
		foreach ($this->_connections as $connection) {
			$connection->close();
		}
	}

}

