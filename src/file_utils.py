"""
File Utilities for Databricks Payer Data Pipeline
==================================================

This module contains all file download, extraction, and organization utilities
for the Databricks medallion architecture pipeline.

All functions are designed to run on Databricks compute nodes with proper
access to Unity Catalog volumes.
"""

import requests
import zipfile
import io
import os
import tempfile
import shutil
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def create_volume_directories(spark, catalog, bronze_db):
    """
    Create the required Unity Catalog volume and directory structure.
    
    Args:
        spark: Spark session
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        dict: Status of directory creation operations
    """
    results = {
        'volume_created': False,
        'directories_created': [],
        'errors': []
    }
    
    try:
        # Create the volume
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze_db}.payer")
        results['volume_created'] = True
        
        # Create directory structure using dbutils
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        directories = [
            f"/Volumes/{catalog}/{bronze_db}/payer/files/claims",
            f"/Volumes/{catalog}/{bronze_db}/payer/files/diagnosis", 
            f"/Volumes/{catalog}/{bronze_db}/payer/files/procedures",
            f"/Volumes/{catalog}/{bronze_db}/payer/files/members",
            f"/Volumes/{catalog}/{bronze_db}/payer/files/providers",
            f"/Volumes/{catalog}/{bronze_db}/payer/downloads"
        ]
        
        for directory in directories:
            try:
                dbutils.fs.mkdirs(directory)
                results['directories_created'].append(directory)
            except Exception as e:
                results['errors'].append(f"Failed to create {directory}: {str(e)}")
        
        return results
        
    except Exception as e:
        results['errors'].append(f"Volume creation failed: {str(e)}")
        return results


@udf(returnType=StringType())
def download_and_extract_files_udf(url, catalog, bronze_db):
    """
    UDF to download and extract ZIP file on Databricks compute nodes.
    
    This function runs on Databricks executor nodes which have access 
    to Unity Catalog volumes and can perform file operations.
    
    Args:
        url (str): URL of the ZIP file to download
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        str: Status message with detailed results
    """
    try:
        # Step 1: Download the ZIP file
        response = requests.get(url)
        response.raise_for_status()
        
        # Step 2: Use temporary directory for extraction (avoids permission issues)
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract ZIP file to temporary directory first
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
            zip_file.extractall(temp_dir)  # ✅ This works in temp directory
            
            results = []
            results.append(f"Successfully extracted ZIP to temporary directory")
            
            # Step 3: Define file mappings (source -> destination)
            file_mappings = {
                "claims.csv": ("claims", "claims.csv"),
                "diagnoses.csv": ("diagnosis", "diagnosis.csv"), 
                "procedures.csv": ("procedures", "procedures.csv"),
                "member.csv": ("members", "members.csv"),
                "providers.csv": ("providers", "providers.csv")
            }
            
            # Step 4: Copy files to Databricks volumes using content-based approach
            for source_file_name, (folder_name, dest_file_name) in file_mappings.items():
                temp_file_path = f"{temp_dir}/{source_file_name}"
                
                if os.path.exists(temp_file_path):
                    # Read file content from temp directory
                    with open(temp_file_path, 'r', encoding='utf-8') as f:
                        file_content = f.read()
                    
                    # Define destination path in volumes
                    dest_path = f"/Volumes/{catalog}/{bronze_db}/payer/files/{folder_name}/{dest_file_name}"
                    parent_dir = f"/Volumes/{catalog}/{bronze_db}/payer/files/{folder_name}"
                    
                    try:
                        # Ensure directory exists by creating a test file
                        test_path = f"{parent_dir}/.test"
                        with open(test_path, 'w') as test_f:
                            test_f.write("test")
                        os.remove(test_path)  # Clean up test file
                        
                        # Write the actual file content to volume
                        with open(dest_path, 'w', encoding='utf-8') as dest_f:
                            dest_f.write(file_content)
                        
                        results.append(f"Successfully copied {source_file_name} to {dest_path}")
                        
                    except Exception as write_error:
                        results.append(f"Error writing {source_file_name}: {str(write_error)}")
                else:
                    results.append(f"Warning: {source_file_name} not found in ZIP")
        
        return "\n".join(results)
        
    except Exception as e:
        import traceback
        return f"Error in UDF: {str(e)}\nTraceback: {traceback.format_exc()}"


def download_and_extract_with_dbutils(spark, url, catalog, bronze_db):
    """
    Alternative file extraction method using dbutils for maximum reliability.
    
    Args:
        spark: Spark session
        url (str): URL of the ZIP file to download
        catalog (str): Unity Catalog name  
        bronze_db (str): Bronze database/schema name
    
    Returns:
        str: Status message with results
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        # Download and extract to local temp first
        response = requests.get(url)
        response.raise_for_status()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract to temp
            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
            zip_file.extractall(temp_dir)
            
            # Define file mappings
            file_mappings = {
                "claims.csv": ("claims", "claims.csv"),
                "diagnoses.csv": ("diagnosis", "diagnosis.csv"), 
                "procedures.csv": ("procedures", "procedures.csv"),
                "member.csv": ("members", "members.csv"),
                "providers.csv": ("providers", "providers.csv")
            }
            
            results = []
            for source_file_name, (folder_name, dest_file_name) in file_mappings.items():
                temp_file_path = f"{temp_dir}/{source_file_name}"
                if os.path.exists(temp_file_path):
                    dest_path = f"/Volumes/{catalog}/{bronze_db}/payer/files/{folder_name}/{dest_file_name}"
                    
                    # Use dbutils to copy file (most reliable method)
                    dbutils.fs.cp(f"file://{temp_file_path}", dest_path, recurse=True)
                    results.append(f"Copied {source_file_name} to {dest_path}")
            
            return "\n".join(results)
            
    except Exception as e:
        return f"Error with dbutils approach: {str(e)}"


def extract_payer_data(spark, catalog="my_catalog", bronze_db="payer_bronze", 
                      url="https://github.com/bigdatavik/databricksfirststeps/blob/6b225621c3c010a2734ab604efd79c15ec6c71b8/data/Payor_Archive.zip?raw=true"):
    """
    Main function to extract payer data files to Unity Catalog volumes.
    
    This is the primary function to call from your notebook for file extraction.
    
    Args:
        spark: Spark session
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
        url (str): URL of the ZIP file containing payer data
    
    Returns:
        dict: Results of the extraction process
    """
    print("🚀 Starting payer data extraction...")
    print("=" * 60)
    
    # Step 1: Create directories
    print("📁 Creating volume directories...")
    dir_results = create_volume_directories(spark, catalog, bronze_db)
    
    if dir_results['errors']:
        print("⚠️ Directory creation warnings:")
        for error in dir_results['errors']:
            print(f"   {error}")
    
    print(f"✅ Created {len(dir_results['directories_created'])} directories")
    
    # Step 2: Extract files using UDF approach
    print("\n📦 Extracting files using UDF...")
    df = spark.createDataFrame([(url, catalog, bronze_db)], ["url", "catalog", "bronze_db"])
    result_df = df.select(download_and_extract_files_udf("url", "catalog", "bronze_db").alias("result"))
    result = result_df.collect()[0]["result"]
    
    print("UDF Results:")
    print(result)
    
    # Step 3: Fallback to dbutils if UDF fails
    if "Error" in result:
        print("\n🔄 UDF failed, trying dbutils fallback...")
        dbutils_result = download_and_extract_with_dbutils(spark, url, catalog, bronze_db)
        print("DBUtils Results:")
        print(dbutils_result)
        final_result = dbutils_result
    else:
        final_result = result
    
    # Step 4: Return results summary
    success = "Error" not in final_result
    
    return {
        'success': success,
        'directory_creation': dir_results,
        'extraction_result': final_result,
        'method_used': 'dbutils' if "Error" in result else 'udf'
    }
