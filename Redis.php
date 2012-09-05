<?php
/**
 * Created by silentium
 * Date: 01.09.12
 * Time: 16:20
 */

class RedisProxy {

	// object pool for sources implementation
	protected static $_pool = array();

	/**
	 * @static
	 * @return RedisProxy
	 */
	public static function getInstance() {
		return end(self::$_pool);
	}

	/**
	 * get number of data sources in the pool
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


	protected $_key;
	public $namespace = '';
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
		$this->_key = count(self::$_pool) - 1;
		$this->_is64 = PHP_INT_SIZE == 8;
	}

	public function __destruct() {
		$this->close();
		unset(self::$_pool[$this->_key]);
	}

	/**
	 * get index of this connection in the pool
	 * @return int
	 */
	public function getKey() {
		return $this->_key;
	}


	/**
	 * Connect to server and add it to source
	 * $weight is used to determine relative amount of stored keys
	 * @param string $host
	 * @param int $port
	 * @param int $weight
	 */
	public function addServer($host = '127.0.0.1', $port = 6379, $weight = 1) {
		$redis = new Redis();
		$connection_string = $host . ':' . $port;
		$redis->connect($host, $port);
		$this->_connections[$connection_string] = $redis;
		$this->_weights[$connection_string] = $weight;
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
	 * @param $name
	 * @return Redis
	 */
	public function getConnectionByKeyName($name) {
		// If we have only one backend, return it.
		if (count($this->_connections) == 1) {
			return $this->_connections;
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

		if (!is_callable(array('Redis', $method))) {
			throw new Exception("Method '$method' does not exist for phpredis");
		}
		if (!isset($args[0]) || !is_string($args[0])) {
			throw new Exception("Only methods with \$key as first parameter can be overloaded");
		}
		if (empty($this->_connections)) {
			throw new RedisException("No active redis servers");
		}

		$redis = $this->getConnectionByKeyName($args[0]);

		return call_user_func_array(array($redis, $method), $args);
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

