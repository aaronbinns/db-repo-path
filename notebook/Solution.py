# Databricks notebook source
# MAGIC %md # Solution
# MAGIC 
# MAGIC Simple notebook to demonstrate the solution to the problem where arbitrary paths within a git repo are not in the `sys.path` on the worker nodes.  
# MAGIC This notebook assumes that the git repo `db-repo-path` is checked out in the user's home directory in Databricks repos.  
# MAGIC That is, it is in: `/Workspace/Repos/{username}/db-repo-path/`

# COMMAND ----------

# MAGIC %md ## Init script
# MAGIC 
# MAGIC For the solution, we need to be able to know, in Python code, wether or not that code is being executed on the driver node or worker nodes.
# MAGIC 
# MAGIC I couldn't figure out a way to do that, other than to have an init-script touch a file when being executed on the driver node.
# MAGIC 
# MAGIC ```
# MAGIC #!/bin/bash
# MAGIC 
# MAGIC if [[ $DB_IS_DRIVER = "TRUE" ]]; then
# MAGIC   touch /tmp/db_is_driver
# MAGIC fi
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Setup
# MAGIC 
# MAGIC You can put the following cells into a stand-alone `setup` notebook, to be `%run` from other notebooks
# MAGIC 
# MAGIC The following cells will
# MAGIC * Force the Python interpreter to create the ephemeral NFS drive for the virtualenv that is shared by all nodes in the cluster
# MAGIC * Find the path to the virtualenv on the ephemeral NFS drive
# MAGIC * Create a .pth file with a line to append the path to the git repo's `src` subfolder to the worker node's `sys.path`
# MAGIC * Restart the python interpreter on all the nodes in the cluster, which will execute the .pth file we just created
# MAGIC * Update the driver's `sys.path` to include the git repo's `src` subfolder

# COMMAND ----------

# Do a trivial %pip install to force the creation of the "ephemeral" NFS drive that will be shared by *all* the nodes in the cluster,
#  and where the notebook-specific virtualenv will be created.
%pip install pandas

# COMMAND ----------

# Once the %pip install is done, the per-notebook virtualenv will be under a directory with part of the path containing the substring "ephemeral"
# We are going to modify a file in that directory, so grab it from the existing `sys.path`
import sys
path = [x for x in sys.path if "ephemeral_nfs" in x][0]
print( path )

# COMMAND ----------

# Get the username for use below
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print( username )

# COMMAND ----------

# Append some lines to the `sites.pth` file in the directory we got above
# This will case the user's `/Workspace/Repos/...` path to be added to the `sys.path` on the *workers* (and only the workers) the next time the
# Python interpreter is restarted.  Notice that we use that `db_is_driver` file that we created in the init script.
with open( f"{path}/sites.pth", 'a') as sites:
    sites.write( "\n" )
    sites.write( f"import sys, os ; [sys.path.append(x) for x in ['/Workspace/Repos/{username}/db-repo-path/src'] if not os.path.exists('/tmp/db_is_driver')]\n" )

# COMMAND ----------

# Once we've updated that file, we have to restart the Python interpreter
# This will restart the Python interpreter on the driver node and all of the worker nodes
dbutils.library.restartPython()

# COMMAND ----------

# Because we restarted the Python interpreter (on driver and all executors) in previous cell, we have to re-compute the username
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Lastly, add the user's Repo path to the `sys.path` on the driver node
import sys ; sys.path.append(f"/Workspace/Repos/{username}/db-repo-path/src")

# COMMAND ----------

# MAGIC %md ## Testing
# MAGIC 
# MAGIC Now that the git repo's `src` subfolder has been added to Python `sys.path` on all the nodes in the cluster, we can try calling our custom Python code, just like we did in the `Problem` notebook.  But this time, all the tests will pass! :)

# COMMAND ----------

# MAGIC %md ### Test 1 - Call custom Python module when running on Driver node
# MAGIC 
# MAGIC By adding the full path to the `db-repo-path` repository's subdirectory `src` to the `sys.path`, the Python interpreter running on the Driver node can successfully call our custom module `greetings.hello`

# COMMAND ----------

import greetings.hello

print( greetings.hello.hello_world() )

# COMMAND ----------

# MAGIC %md ### Test 2 - Call custom Python module when running on Worker node
# MAGIC 
# MAGIC Because we've modified the `sys.path` on all the worker nodes, the git repos `src` subfolder is in that path and the custom Python code can be found when the UDF is executed on the worker.
# MAGIC 
# MAGIC You can put the `import greetings.hello` inside of the UDF, or you can do it before the UDF definition.  Both will work.

# COMMAND ----------

# This will print out "Hello world" 6 times

# You can import the custom Python module inside the UDF, if that is your preferred Python coding style
def test_import(unused):
    import greetings.hello
    return greetings.hello.hello_world()
    
sc.parallelize(["foo","bar","baz","frotz","bizzle","fizzle"]).map(test_import).collect()

# COMMAND ----------

# This will print out "Hello world" 6 times

# You can import the custom Python module before defining the UDF
import greetings.hello

def test_import(unused):
    return greetings.hello.hello_world()
    
sc.parallelize(["foo","bar","baz","frotz","bizzle","fizzle"]).map(test_import).collect()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Utility functions
# MAGIC 
# MAGIC These were just some cells that were handy to have around for testing/debugging the code above

# COMMAND ----------

# Just get the sys.path from the worker nodes and print it out.
# This is useful to sanity-check that the user's `/Workspace/Repos/...` is in the `sys.path` on the workers
import sys

def get_sys_path(foo):
    return sys.path

sc.parallelize(["e"]).map(get_sys_path).collect()

# COMMAND ----------

# A simple test to verify that we can import a custom Python module on the worker
# This should print "None".  If this code fails to run, then the `sys.path` hasn't been setup correctly on the worker node
def test_import(foo):
    import greetings.hello
    
sc.parallelize(["whatever"]).map(test_import).collect()

# COMMAND ----------

# Simple test/sanity-check if the `db_is_driver` file is on the workers -- it should not be on the workers, only the driver
# This should print "False" three times
def driver_check(foo):
    import os.path
    return os.path.exists('/tmp/db_is_driver')
    
sc.parallelize(["foo", "bar", "baz"]).map(driver_check).collect()

# COMMAND ----------


