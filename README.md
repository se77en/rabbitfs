#RabbitFS - A distributed file storage based on the [Facebook Haystack Paper](https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf)
![](https://travis-ci.org/lilwulin/rabbitfs.svg)

##Intro
RabbitFS is a distributed file storage implementing the Facebook's Haystack paper.
The key point of Haystack is to append small files to a large volume file, and retrieve the
small file from volume file using the offset and size. The reason of this design is to reduce the number
of disk operation to read i-node.

*Warning: I only create this project for fun. It hasn't been tested in real production environment, if you want to use open source implementation of Haystack, check out [chrislusf](https://github.com/chrislusf)'s AWESOME [Seaweedfs](https://github.com/chrislusf/seaweedfs)*

##Architecture
Following Haystack, RabbitFS has two major components: **Directory Server** and **Store Server**.

**Directory Server - **When uploading a file, client asks directory to assign a file id, and a
store server's address. Directory will randomly select a store server, and use a volume id, uuid, and a random number(cookie) to construct a file id. Directory Server also periodically polling the store servers' status

**Store Server - **Store Server manages multiple volume files, and handles client's read, write, delete operation.

##Usage
###Start Server
```bash
# help
./rabbitfs directory -h
./rabbitfs store -h

# run server
./rabbitfs directory # default address: 127.0.0.1:9666, default configuration path: /etc/rabbitfs/
./rabbitfs store # default address: 127.0.0.1:8666, default configuration path and volume path: /etc/rabbitfs/
```
###Create Volume
```bash
curl http://127.0.0.1:9666/vol/create
{"id":3,"ip":["127.0.0.1:8666"]}
```
###File Operation
```bash
# ask for file id and store server's address
curl http://127.0.0.1:9666/dir/assign
{"fileid":"1,15800990509173573693,4167969108","volume_ip":"127.0.0.1:8666"}

# use the fileid to upload file
curl -F "action=upload" -F "filename=@/path/to/file" http://127.0.0.1:8666/1,15800990509173573693,4167969108
{"name":"filename","size":84458}
# now you can use the url http://127.0.0.1:8666/1,15800990509173573693,4167969108 to get the file

# use the fileid to delete file
curl http://127.0.0.1:8666/del/1,15800990509173573693,4167969108
```

##Configuration
RabbitFS will read the JSON file named *rabbitfs.conf.json* under the configuration path. You can specify the configuration path when you run the server.

**configuration example**
```json
{
	"directory": [
		"127.0.0.1:9331",
		"127.0.0.1:9332",
		"127.0.0.1:9333"
	],
	"store": [
		"127.0.0.1:8787",
		"127.0.0.1:8788",
		"127.0.0.1:8789"
	]
}
```

##Replication
Specify the replication number when ask directory to create volume, and directory will create volume on replication number of store servers. the volume id is mapped to multiple server address.
When being asked to assign a file id with replication number, directory will randomly choose the volume with replication number.
When the file with this file id gets uploaded to a store server, the store server will replicate this file to other server's volume with the same volume id.

**Example:**
```bash
curl http://127.0.0.1:9333/vol/create?replication=2
{"id":4,"ip":["127.0.0.1:8666","127.0.0.1:8667"]}
curl http://127.0.0.1:9333/dir/assign?replication=2
```

##Other Details
###Raft
Directory Server can have multiple peers, using a distributed protocol called Raft. All http request will be redirected to leader peer. For more details, please check out the [Raft paper](https://raftconsensus.github.io/).

###Needle
A needle wraps a small file with some necessary data. When uploading a file, it's actually the needle gets appended into volume file.

###File ID
The format of file id is: `<volume id>,<needle id>,<cookie>`

#LICENSE
The MIT License (MIT)

Copyright (c) 2015 Wu Lin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
