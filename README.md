# redis-tagging-locking-sample


Sample code to use Redis as a tagging cache and Distributed Lock Manager.

## Usage

Run multiple instances of the *DistributedLockingPerMethod* console app to see a simulation of Redis as a distributed lock manager.
Examine the tests in *RedisWithTaggingAndLockingTests* and run them using your favorite test runner for examples of how to consume the Tagging and Locking extensions.

## Notes

This project is intended to give a general idea of how tagging and DLM can be accomplished with Redis and .NET. Many specific functions (Removing cache entries by tag) have not been included in this sample, but could be easily accomplished with modified versions of the included C# code and Lua scripts.  
If you do not wish to install the PostSharp extension in Visual Studio, you can find a pre-compiled version of DistributedLockingPerMethod.exe in the top level "bin" folder.
Be sure to update the connection info for your Redis server!

## Dependencies

ServiceStack.Redis 4.0.33, NUnit 2.6.3, PostSharp 4.0.39, PostSharp Visual Studio Extension*

## License

Copyright 2014 Stackify, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This code extends some portions of the ServiceStack.Redis project under the terms granted in the FOSS exceptions segment of [that project's license file](https://github.com/ServiceStack/ServiceStack.Redis/blob/v4.0.33/license.txt "ServiceStack.Redis License").
