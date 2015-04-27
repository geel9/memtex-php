<?php

namespace Memtex;

class Memtex {
	private $mutexes = array();
	private $instanceID = -1;

	public $memcachedClient;

	public $keyPrefix = "memtex-";

	public $sleepTime = 10000;

	public function __construct(\Memcached $client){
		$this->memcachedClient = $client;
		$this->$instanceID = mt_rand(0, 2147483646); //Should probably (definitely) use a UUID in future.
	}

	public function Acquire($name, $timeout = 1, $lifetime = 5){
		if($this->HasAcquired($name)) return true;

		$memcachedLifetime = ($lifetime == -1) ? 0 : $lifetime; //0 is "infinite length" for memcached.

		$instanceID = $this->$instanceID;
		$curMTime = microtime(true);
		$key = $this->keyPrefix . $name;

		$currentCas = "";

		while(microtime(true) - $curMTime < $timeout){

			$curHolder = $this->memcachedClient->get($key, null, $currentCas);
			if(!($curHolder === FALSE || $curHolder === null || $curHolder == $instanceID)){
				usleep($this->sleepTime);
				continue;
			}

			//currentCas will not be empty if a mutex is released but not expired. Add will still fail even though we can acquire the mutex.
			if(!empty($currentCas)){
				$acquired = $this->memcachedClient->cas($currentCas, $key, $instanceID, $memcachedLifetime);
			}
			else{
				$acquired = $this->memcachedClient->add($key, $instanceID, $memcachedLifetime);
			}


			$cas = "";
			$stillSame = $this->memcachedClient->get($key, null, $cas) == $instanceID;

			if($acquired && $stillSame){
				if(!$this->InArray($name)){
					$newMutex = array("name" => $name, "time" => microtime(true), "acquired" => true, "lifetime" => $lifetime, "token" => $cas);
					$this->mutexes[] = $newMutex;
				} else{
					$this->SetAcquired($name, $lifetime, true, $cas);
				}
				return true;
			}
			usleep($this->sleepTime);
		}
		return false;
	}

	public function Release($name){
		if(!$this->HasAcquired($name)){
			$this->SetAcquired($name, 0, false, null);
			return;
		}
		$mutex = $this->GetByName($name);
		$this->memcachedClient->cas($mutex['token'], $this->keyPrefix . $name, null, 1);

		$this->SetAcquired($name, 0, false, 0);
	}

	private function InArray($name){
		return $this->GetByName($name) !== FALSE;
	}

	private function GetByName($name){
		foreach($this->$mutexes as &$mutex){
			if($mutex['name'] == $name) return $mutex;
		}
		return FALSE;
	}

	private function HasAcquired($name){
		$mutex = $this->GetByName($name);
		if($mutex === FALSE) return FALSE;
		if(microtime(true) - $mutex['time'] < $mutex['lifetime'] && $mutex['acquired']) return TRUE;

		return FALSE;
	}

	private function SetAcquired($name, $lifetime, $acquired, $cas){
		foreach($this->mutexes as $key => $mutex){
			if($mutex['name'] == $name){
				if($acquired) {
					$mutex['time'] = microtime(true);
					$mutex['lifetime'] = $lifetime;
					$mutex['token'] = $cas;
				}
				$mutex['acquired'] = $acquired;
				$this->mutexes[$key] = $mutex;
			}
		}
	}
} 