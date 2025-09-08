"""
Debugging Utilities for Databricks Payer Data Pipeline
======================================================

This module contains debugging and troubleshooting functions
for diagnosing issues with file extraction and volume access.
"""

import os
import shutil
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def debug_directory_structure_udf(catalog, bronze_db):
    """
    Debug UDF: Lists the complete directory structure for troubleshooting.
    
    Args:
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        str: Complete directory tree listing
    """
    try:
        base_volume = f"/Volumes/{catalog}/{bronze_db}/payer"
        results = []
        results.append("=== DIRECTORY STRUCTURE DEBUG ===")
        
        # Check if base volume exists
        if os.path.exists(base_volume):
            results.append(f"✅ Base volume exists: {base_volume}")
            
            # Walk through directory structure
            for root, dirs, files in os.walk(base_volume):
                level = root.replace(base_volume, '').count(os.sep)
                indent = ' ' * 2 * level
                results.append(f"{indent}📁 {os.path.basename(root)}/")
                
                # List files
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        file_size = os.path.getsize(file_path)
                        results.append(f"{subindent}📄 {file} ({file_size:,} bytes)")
                    except:
                        results.append(f"{subindent}📄 {file} (size unknown)")
        else:
            results.append(f"❌ Base volume does not exist: {base_volume}")
        
        return "\n".join(results)
        
    except Exception as e:
        return f"❌ Directory debug failed: {str(e)}"


@udf(returnType=StringType())
def cleanup_files_udf(catalog, bronze_db):
    """
    Cleanup UDF: Removes all extracted files (use for testing fresh extractions).
    
    Args:
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        str: Cleanup operation results
    """
    try:
        base_path = f"/Volumes/{catalog}/{bronze_db}/payer"
        results = []
        results.append("=== CLEANUP OPERATION ===")
        
        directories_to_clean = ["downloads", "files"]
        
        for dir_name in directories_to_clean:
            dir_path = f"{base_path}/{dir_name}"
            if os.path.exists(dir_path):
                try:
                    shutil.rmtree(dir_path)
                    results.append(f"✅ Removed directory: {dir_name}")
                except Exception as e:
                    results.append(f"⚠️ Failed to remove {dir_name}: {str(e)}")
            else:
                results.append(f"ℹ️ Directory not found: {dir_name}")
        
        results.append("🧹 Cleanup completed. Re-run extraction if needed.")
        return "\n".join(results)
        
    except Exception as e:
        return f"❌ Cleanup failed: {str(e)}"


def debug_permissions(spark, catalog, bronze_db):
    """
    Debug permission issues with Unity Catalog volumes.
    
    Args:
        spark: Spark session
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        dict: Permission debugging results
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        print("🔐 PERMISSION DEBUGGING")
        print("=" * 40)
        
        results = {
            'catalog_accessible': False,
            'schema_accessible': False,
            'volume_accessible': False,
            'write_permissions': False,
            'errors': []
        }
        
        # Test catalog access
        try:
            spark.sql(f"USE CATALOG {catalog}")
            print(f"✅ Catalog accessible: {catalog}")
            results['catalog_accessible'] = True
        except Exception as e:
            print(f"❌ Catalog access failed: {str(e)}")
            results['errors'].append(f"Catalog: {str(e)}")
        
        # Test schema access
        try:
            spark.sql(f"USE SCHEMA {bronze_db}")
            print(f"✅ Schema accessible: {bronze_db}")
            results['schema_accessible'] = True
        except Exception as e:
            print(f"❌ Schema access failed: {str(e)}")
            results['errors'].append(f"Schema: {str(e)}")
        
        # Test volume access
        try:
            volume_path = f"/Volumes/{catalog}/{bronze_db}/payer"
            dbutils.fs.ls(volume_path)
            print(f"✅ Volume accessible: {volume_path}")
            results['volume_accessible'] = True
        except Exception as e:
            print(f"❌ Volume access failed: {str(e)}")
            results['errors'].append(f"Volume: {str(e)}")
        
        # Test write permissions
        try:
            test_file = f"/Volumes/{catalog}/{bronze_db}/payer/.permission_test"
            dbutils.fs.put(test_file, "test content", overwrite=True)
            dbutils.fs.rm(test_file)
            print("✅ Write permissions confirmed")
            results['write_permissions'] = True
        except Exception as e:
            print(f"❌ Write permission test failed: {str(e)}")
            results['errors'].append(f"Write permissions: {str(e)}")
        
        # Summary
        all_good = all([
            results['catalog_accessible'],
            results['schema_accessible'], 
            results['volume_accessible'],
            results['write_permissions']
        ])
        
        if all_good:
            print("\n🎉 All permissions look good!")
        else:
            print("\n⚠️ Permission issues detected. Check errors above.")
        
        results['all_permissions_ok'] = all_good
        return results
        
    except Exception as e:
        print(f"❌ Permission debugging failed: {str(e)}")
        return {'all_permissions_ok': False, 'errors': [str(e)]}


def debug_file_details(spark, catalog, bronze_db, file_path=None):
    """
    Get detailed information about specific files or all files.
    
    Args:
        spark: Spark session
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
        file_path (str, optional): Specific file to debug
    
    Returns:
        dict: Detailed file information
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        print("🔍 FILE DETAILS DEBUG")
        print("=" * 40)
        
        results = {'files': {}, 'errors': []}
        
        if file_path:
            # Debug specific file
            files_to_check = [file_path]
        else:
            # Debug all expected files
            base_path = f"/Volumes/{catalog}/{bronze_db}/payer/files"
            files_to_check = [
                f"{base_path}/claims/claims.csv",
                f"{base_path}/diagnosis/diagnosis.csv",
                f"{base_path}/procedures/procedures.csv",
                f"{base_path}/members/members.csv",
                f"{base_path}/providers/providers.csv"
            ]
        
        for file_path in files_to_check:
            try:
                # Get file info using dbutils
                file_info = dbutils.fs.ls(file_path)
                if file_info:
                    info = file_info[0]
                    file_size = info.size
                    mod_time = info.modificationTime
                    
                    print(f"📄 {file_path}:")
                    print(f"   Size: {file_size:,} bytes")
                    print(f"   Modified: {mod_time}")
                    
                    # Try to read first few lines for content validation
                    try:
                        content = dbutils.fs.head(file_path, max_bytes=500)
                        lines = content.split('\n')
                        print(f"   Header: {lines[0] if lines else 'Empty'}")
                        print(f"   Sample: {lines[1] if len(lines) > 1 else 'No data'}")
                    except Exception as read_e:
                        print(f"   Content read error: {str(read_e)}")
                    
                    results['files'][file_path] = {
                        'size': file_size,
                        'modification_time': mod_time,
                        'accessible': True
                    }
                else:
                    print(f"❌ {file_path}: Not found")
                    results['files'][file_path] = {'accessible': False}
                    
            except Exception as e:
                print(f"❌ {file_path}: Error - {str(e)}")
                results['errors'].append(f"{file_path}: {str(e)}")
        
        return results
        
    except Exception as e:
        print(f"❌ File details debug failed: {str(e)}")
        return {'errors': [str(e)]}


def run_full_diagnostics(spark, catalog, bronze_db):
    """
    Run comprehensive diagnostics to identify any issues.
    
    Args:
        spark: Spark session
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        dict: Complete diagnostic results
    """
    print("🔧 RUNNING FULL DIAGNOSTICS")
    print("=" * 50)
    
    diagnostics = {}
    
    # 1. Permission check
    print("\n1️⃣ Checking permissions...")
    diagnostics['permissions'] = debug_permissions(spark, catalog, bronze_db)
    
    # 2. File details
    print("\n2️⃣ Checking file details...")
    diagnostics['files'] = debug_file_details(spark, catalog, bronze_db)
    
    # 3. Directory structure (using UDF)
    print("\n3️⃣ Checking directory structure...")
    try:
        test_df = spark.createDataFrame([(catalog, bronze_db)], ["catalog", "bronze_db"])
        dir_result = test_df.select(
            debug_directory_structure_udf("catalog", "bronze_db").alias("structure")
        ).collect()[0]["structure"]
        print(dir_result)
        diagnostics['directory_structure'] = dir_result
    except Exception as e:
        error_msg = f"Directory structure check failed: {str(e)}"
        print(f"❌ {error_msg}")
        diagnostics['directory_structure'] = error_msg
    
    # Summary
    print("\n" + "=" * 50)
    print("🎯 DIAGNOSTIC SUMMARY:")
    
    issues_found = []
    if not diagnostics.get('permissions', {}).get('all_permissions_ok', False):
        issues_found.append("Permission issues")
    if diagnostics.get('files', {}).get('errors'):
        issues_found.append("File access issues")
    if "failed" in diagnostics.get('directory_structure', ''):
        issues_found.append("Directory structure issues")
    
    if not issues_found:
        print("🎉 No issues detected! System appears to be working correctly.")
    else:
        print(f"⚠️ Issues found: {', '.join(issues_found)}")
        print("💡 Review the detailed output above for troubleshooting steps.")
    
    diagnostics['issues_found'] = issues_found
    diagnostics['overall_health'] = len(issues_found) == 0
    
    return diagnostics
