CreateCacheAreaFromReplica
==========================

Create update areas for replica from replica syncronization

Class is developed against ArcGIS 10.1 arcpy
Many ArcGIS server instances rely on replicas as the registered datasource.
Many services that rely on the data are cached.
This class provides the architecture for identifying and documenting the changes for each replica in the DB
as a generalized features in a feature class.

Class will log to Log file and email when errors are encountered

To run, initialize the class, set the parameters, and execute the functions in logical order:
ExtractDeltas(queries all replica changes from databases)
BufferDeltas (Generalizes all changes into grids)
LoadDeltas (appends to Changes FC)
TrySync (Connected sync due to bug in 10.1. Should execute emmediately  after completion of previous in order to not miss edits)
Update_Statistics (Compress & stats)

See architecture diagram for flow

Solution can be improved by: updating for bug fixes
    Use delta GDB to commit changes to replica = no need for TrySync (connected)
    Use TableToNumPyArray
  Improve on fishnet to Use super tiles from Map Server Cache Tiling Scheme To Polygons tool

