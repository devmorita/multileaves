# Multi-thread script for Leafee (not official)

This is the multi-thread program to communicate with leafees those are the magnetic open-closed door sensor (BLE).

----

## Get Started

### 1. Requirements

* `H/W : At least one leafee device`
* `S/W : Python2.7`
* `S/W : bluepy version 1.2.0`
```
 # apt-get install python-pip libglib2.0-dev
 # pip install bluepy==1.2.0
```

### 2. Optional

* `M/W : sqlite3`
  This software can log into sqlite database file.
  It automatically creates the database in /tmp/sqlite/sensor.db,
```
# apt-get install sqlite3
```

* `M/W : firebase-admin`
  This software can log into firegbase database.
```
# pip install firebase-admin
```

### 2. Before Starting

 Investigate the leafee device MAC address.
 Here is the example how to find it by bluz by Raspberry Pi 2/3/ZeroW/ZeroWH
 leafee is shown as MA
```
 # apt-get install bluez
 # bluetoothctl
   [bluetooth]# scan on 
      [NEW] Device xx:xx:xx:xx:xx:xx MA
   [bluetooth]# scan off 
   [bluetooth]# scan on
   [bluetooth]# quit
```
 Prepare the config.json file.
 See config.json.example or config.json.readme

### 3. Run
Here is the usage.
```
USAGE   : python　multileaves.py
```

### 4. Stop

There is no stop interface, then stop by 
```
Ctrl + C
```
or 
find the process id of the runnning process then kill such as 
```
$ ps -eaf | grep simpleleaf
$ kill xxxx
```
----

## Importance and Limitations

* This is not official software of leafee. And "leafee" is a trademake of Strobo Inc.
* DISCLAIMER OF WARRANTY
  Use of this software is at your risk. All materials, information, products, software, programs are provided "AS IS" with no warranties.
  You agree to do so at your own risk and you will be responsible for any damages that may happen, including loss of data or damage your computer system, the equivalent.

----

## License
----
 MIT

