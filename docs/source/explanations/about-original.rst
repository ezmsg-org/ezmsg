How ``ezmsg`` works
===================

When an ``ezmsg`` pipeline is started, ``ezmsg`` initializes two additional processes to manage execution. The first is the SharedMemoryServer, which allocates blocks of memory to nodes. The second is the GraphServer, which keeps track of the graph state during execution. One GraphServer should be initialized per ezmsg pipeline, and one SharedMemoryServer should be initialized per machine which the pipeline is running on. Check out the Other page for more information on managing the SharedMemoryServer and GraphServer during pipeline execution.