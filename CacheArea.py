
# ---------------------------------------------------------------------------
# UpdateLandbaseReplica.py
# Created on: 2012-09-26
# Updated on: 2013-01-28
# Description:
# Synchronizer class that will accept parameter inputs. for connection, replica, log file, etc  
# Has logging and email notification coded in.
# Will export to delta file GDB, Generate areas where updates occur, then synchronize child GDB
#
# Process:
#   1.Create Delta GDB in a Temp directory
#   2.Read GDB_DataChangesDatasets table for list of feature classes
#   3.Selects deleted features from each layer in the child DB using a where clause built from delete table in delta GDB
#   4.buffers resulting selection and appends to buffers FC
#   5.buffers all features in each FC in the Delta GDB and appends buffers to FC
#   6.Intersects buffer with Grid feature class to generalize update areas
#   7.Loads generalized update area to an update feture class for future reference
#   9.Starts a connected synchronization - ESRI SYNC BUG "CANNOT IMPORT REPLICA CHANGES FROM DELTA GDB"
#   10.Compress and update statistics on replica GDB - Keep DB Tuned!!!!
# ---------------------------------------------------------------------------


# Import system modules
import sys, string, os, arcpy, smtplib, datetime, time
from arcpy import env


class Update(object):
#Synchronizer class
    def __init__(self):

        #Data Sources
        self.parentGDB = "D:\\arcgisserver\\database connections\\amt11p.sde"
        self.childGDB = "D:\\arcgisserver\\database connections\\amtsql2.sde"
        self.replicaName = "AGS_SA.AMTSQL2_Landbase_Replica"
        self.deltaGDB = " " 
		self.ChagesFC = self.childGDB + "\\" + "Cache_Update"

        #Email Notification Config
        self.sender = 'GIS_support@Destination.com'
        self.receivers = ['GIS_support@Destination.com']
        self._msg = []
        self._error = False

        #Log File Location
        self.log = "D:\\arcgisserver\\replication\\LogFile.txt"
		


####################################################################################################

# Get FC Name from Deltas table.
# Will lookup feature class name from deltaGDB.
# Then validate FC in target DB

    def _GetTargetFC(self,deltaGDB,tb):
        #Get table number from name
        try:
            name = str(tb).split("_")
    
            #Query GDB_ChangesDatasets table for feature class name
            env.workspace = str(deltaGDB)
            rows = arcpy.SearchCursor(env.workspace + os.sep + "GDB_DataChangesDatasets","ID = " + name[1])
            row = rows.next()
            
            #if Deletes table has Feature class Name- Some do not
            if row:
                fcName= row.getValue ("Name")
                env.workspace = self.childGDB
                ##Check if feature class exists in replica- should but you never know
                if arcpy.Exists(fcName):
                    d = arcpy.Describe(env.workspace + "\\" + fcName)
                    #Verify existing feature class is a feature class - Some are not
                    if d.DataType == "FeatureClass":
                        del rows
                        return d.catalogPath
                
        except:
            self._msg.append("ERROR Getting FC Class Name From " + str(self.deltaGDB)) 
            self._error = True
            return None

####################################################################################################

# Should use  TableToNumPyArray but have createLookup instead (Bug NIM-087125)
# Will read input table and create array from GLOBALID Field

    def _CreateLookup(self,deltaGDB, tb): 
        try:
            arr = []

            #Open search cursor on table
            env.workspace = str(deltaGDB)
            rows = arcpy.SearchCursor(env.workspace + os.sep + str(tb))

            # read each row and add GUID to array
            for row in rows:
                arr.append (row.getValue("GLOBALID"))

            del rows
            return arr
        except:
            self._msg.append("ERROR creating lookup GUID array for " + str(tb) + " in " + str(self.deltaGDB))
            self._error = True
            return None
        
####################################################################################################

# Build where clause for query when selecting deletes from replica
       
    def _BuildWhereClauseFromList(self,table, field, valueList):
        # Takes a list of values and constructs a SQL WHERE
        # clause to select those values within a given field and table.

        # Add DBMS-specific field delimiters
        fieldDelimited = arcpy.AddFieldDelimiters(arcpy.Describe(table).path, field)

        # Determine field type
        fieldType = arcpy.ListFields(table, field)[0].type

        # Add single-quotes for string field values
        if str(fieldType) == 'String' or str(field) == 'GLOBALID':
            valueList = ["'%s'" % value for value in valueList]

        # Format WHERE clause in the form of an IN statement
        whereClause = "%s IN(%s)" % (fieldDelimited, ', '.join(map(str, valueList)))

        return whereClause
    
####################################################################################################

 #Buffer and append feature class to results feature class

    def _BufferAppend(self,fc, buffers):
        try:
            fcBuffer = arcpy.Buffer_analysis(fc,"in_memory\\buffer",50,"","","ALL")
            arcpy.Append_management(str(fcBuffer),str(buffers), "NO_TEST")
            return True
        except:
            self._msg.append("ERROR  BufferAppend " + str(fc) + " to " + str(buffers))
            self._error = True
            return False


####################################################################################################
         
# Generalize features for Cache creation areas and loads features to target GDB

    def LoadDeltas(self,fc):
        env.overwriteOutput = "True"
        try:
            #Create Fishnet Layer from FC
            index = arcpy.MakeFeatureLayer_management(self.childGDB + "\\" + "Update_Grid" ,"in_memory\\index")
            #Select from fishnet
            selection = arcpy.SelectLayerByLocation_management(index,"INTERSECT",str(fc),"","NEW_SELECTION")
            #Dissolve fishnet
            fcAggregate = arcpy.Dissolve_management (selection, "in_memory\\fcAggregate","","", "SINGLE_PART")

            #ADD TRACKING FIELDS
            arcpy.AddField_management( fcAggregate, "ReplicaName", "TEXT")
            arcpy.AddField_management( fcAggregate, "Date", "DATE")

            #Calculate Tracking Fields
            name = self.replicaName.split("_")
            arcpy.CalculateField_management(fcAggregate, "ReplicaName", "'" + name[2] + "'", "PYTHON" )
            arcpy.CalculateField_management(fcAggregate,"Date","time.strftime('%m/%d/%Y')","PYTHON_9.3","#")

            # Append features to child database
            arcpy.Append_management ("in_memory\\fcAggregate", self.childGDB + "\\" + "Cache_Update" , "NO_TEST")
            self._msg.append("SUCCESS Generalizing and Saving Cache Areas for " + str(self.deltaGDB)+ " " + str(datetime.datetime.now()))
            return str(self.childGDB) + "\\" + "Cache_Update Complete"
        
        except:
            #Log file error message
            self._msg.append("ERROR Generalizing and loading Areas for " + str(self.deltaGDB)+ " " + str(datetime.datetime.now()))
            self._error = True
            return False

####################################################################################################

# Function queries deltaGDB,  Sends feature classes to buffer and load
# Sends tables to lookup, query, and selection from childGDB.  Then Buffer and load.
# Generalize results to simplify Cache areas and upload to target GDB

    def BufferDeltas(self, deltaGDB):
        
        ## Create feature class for all buffers
        env.workspace = str(deltaGDB)
        env.overwriteOutput = "True"

        #Create feature class for outputs
        buffers = arcpy.CreateFeatureclass_management("in_memory","buffers","POLYGON","","","","D:\\arcgisserver\\replication\\WGS84.prj")

        #Get list of tables in deltaGDB
        tbList = arcpy.ListTables()
        try:
            for tb in tbList:
                if tb.endswith("Deletes"):
                    #Get reference to replica FC 
                    target = self._GetTargetFC(deltaGDB, tb)
                    if target:
                        #Build query
                        #Query gets all features slated to be deleted when changes are pushed to the childGDB
                        query = self._BuildWhereClauseFromList(target,"GLOBALID",self._CreateLookup(deltaGDB,tb))

                        # Make layer from replica FC using query
                        fc = arcpy.MakeFeatureLayer_management (target, "in_memory\Selection", query)

                        #Buffer and Append to FC
                        if arcpy.GetCount_management (fc) >= 1 :
                            self._BufferAppend(fc,buffers)
                            print ("Buffer and Append " + str(target))
                        
            # For each feature class in delta GDB buffer and append to fc
            fcList = arcpy.ListFeatureClasses()
            for fc in fcList:
                self._BufferAppend(fc,buffers)
                print ("Buffer and Append " + str(fc))

            #Copy features to temp disc location for evaluation
            arcpy.CopyFeatures_management (buffers,"D:\\Temp\\" + self.replicaName.split(".")[1] + "_" + time.strftime("%Y_%m_%d") + "buffers.shp")

            #Log End
            self._msg.append ("Cache Areas Created for " + str(deltaGDB))
            return str(buffers)
        except:
            self._msg.append ("ERROR creating Cache Areas for " + str(deltaGDB))
            self._error = True
            return False
            
####################################################################################################

    def ExtractDeltas(self):       

        # Process: Synchronize Changes
        env.overwriteOutput = "True"
        try:
            #Export Delta GDB
            deltaGDB = "D:\\Temp\\" + self.replicaName.split(".")[1] + "_" + time.strftime("%Y_%m_%d") + ".gdb"

            arcpy.ExportDataChangeMessage_management (self.parentGDB, deltaGDB, self.replicaName,"","","")
            self.deltaGDB = deltaGDB

            #Log Eend
            self._msg.append("Extract SDE Deltas for " + str(self.replicaName) + " " + str(datetime.datetime.now()))
            return str(self.deltaGDB)
        except:
            #Log file error message        
            self._msg.append("ERROR Extracting " + str(self.replicaName) + " Deltas to " + str(deltaGDB))
            self._error = True
            return False

####################################################################################################

##  def ApplyDeltas(self):
##  ESRI BUG NIM085414- IMPORT CHANGES FAILS WITH UNSPECIFIED ERROR
##  CANNOT IMPORT MESSAGES THROUGH PYTHON AT THIS TIME
##        try:
##            #Define xml file name
##            xmlFile = self.deltaGDB.replace(".gdb",".xml")
##            #Log Start
##            self._msg.append("Import SDE Changes for " +  str(self.replicaName) + " Initiated " +str(datetime.datetime.now()))
##            #Import Deltas to Child GDB and create export message
##            arcpy.ImportMessage_management (self.childGDB, self.deltaGDB, xmlFile ,"IN_FAVOR_OF_IMPORTED_CHANGES")
##
##            #Import message to Parent GDB
##            arcpy.ImportMessage_management (self.parentGDB, xmlFile)
##
##            #Log End
##            self._msg.append("ERROR Importing SDE Changes for " + str(self.replicaName) + " Completed " + str(datetime.datetime.now()))
##            return True

####################################################################################################
    def TrySync(self):
        try:
            
            #Log Start
            #self._msg.append("Import SDE Changes for " +  str(self.replicaName) + " Initiated " + str(datetime.datetime.now()))

            #Synchronize
            arcpy.SynchronizeChanges_management(self.parentGDB, self.replicaName, self.childGDB, "FROM_GEODATABASE1_TO_2", "IN_FAVOR_OF_GDB1", "BY_OBJECT", "DO_NOT_RECONCILE")       
            #Log End
            self._msg.append("Import SDE Deltas for " + str(self.replicaName) + " Completed " + str(datetime.datetime.now()))
            return True

        #LOG ERRORS & NOTIFY
        except:
            #Log file error message
            self._msg.append("ERROR Synchronizing " + str(self.replicaName))
            self._error = True
            return False

####################################################################################################

#   Function compresses database and updates statistics for database tuning and performance.
#   Call after successful synchronization.

    def Update_Statistics(self, workspace):
        env = workspace
        try:
            arcpy.Compress_management(env)
        except:
            self._msg.append("ERROR Compressing Replica")
            self._error = True
            return None
        try:
            arcpy.AnalyzeDatasets_management(env, "SYSTEM", "", "","","")
            
        except:
            self._msg.append("ERROR Updating DB Statistics")
            self._error = True
            return None

        self._msg.append("DB Statistics Completed")
        return True
        
####################################################################################################
#Send email notification containing class errors
    def _Send(self):
        try:
            message = string.join((
            "From: %s" % self.sender,
            "To: %s" % self.receivers,
            "Subject: %s Sync Errors" % self.replicaName ,
            "",
             str(self._msg)
            ), "\r\n")
            smtpObj = smtplib.SMTP('epenotes.intra.epelectric.com')
            smtpObj.sendmail(self.sender, self.receivers, message)
            smtpObj.quit
            return True
        except smtplib.SMTPException:
            self._msg.append("Error Sending Email")
            return False        

####################################################################################################
    def ReportLog (self):
        logFile = open(self.log, 'a')
        for msg in self._msg:
            logFile.write(msg + "\n")
        logFile.write("\n")
        logFile.close
        #Email Error
        if self._error:
            self._Send()    
   
