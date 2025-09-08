"""
Testing Utilities for Databricks Payer Data Pipeline
=====================================================

This module contains comprehensive testing and validation functions
for verifying the success of file extraction and data quality.
"""

import os
import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def validate_extracted_files_udf(catalog, bronze_db):
    """
    Validation UDF: Verifies that all files were extracted correctly
    and provides detailed file information.
    
    Args:
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        str: Detailed validation report
    """
    try:
        base_path = f"/Volumes/{catalog}/{bronze_db}/payer/files"
        
        expected_files = [
            ("claims", "claims.csv"),
            ("diagnosis", "diagnosis.csv"),
            ("procedures", "procedures.csv"),
            ("members", "members.csv"),
            ("providers", "providers.csv")
        ]
        
        results = []
        results.append("=== FILE VALIDATION REPORT ===")
        
        total_files = 0
        successful_files = 0
        total_size = 0
        
        for folder, filename in expected_files:
            file_path = f"{base_path}/{folder}/{filename}"
            
            if os.path.exists(file_path):
                try:
                    # Get file stats
                    file_stats = os.stat(file_path)
                    file_size = file_stats.st_size
                    
                    # Try to read first few lines to validate CSV structure
                    with open(file_path, 'r', encoding='utf-8') as f:
                        first_line = f.readline().strip()
                        line_count = sum(1 for _ in f) + 1  # +1 for the header we already read
                    
                    results.append(f"✅ {folder}/{filename}:")
                    results.append(f"   📁 Size: {file_size:,} bytes")
                    results.append(f"   📊 Rows: ~{line_count:,} (including header)")
                    results.append(f"   📋 Header: {first_line}")
                    
                    successful_files += 1
                    total_size += file_size
                    
                except Exception as read_error:
                    results.append(f"⚠️ {folder}/{filename}: File exists but read error - {str(read_error)}")
            else:
                results.append(f"❌ {folder}/{filename}: File not found")
            
            total_files += 1
        
        # Summary
        results.append(f"\n=== SUMMARY ===")
        results.append(f"📈 Success Rate: {successful_files}/{total_files} files ({successful_files/total_files*100:.1f}%)")
        results.append(f"💾 Total Data Size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
        
        if successful_files == total_files:
            results.append("🎉 All files successfully extracted and validated!")
        else:
            results.append("⚠️ Some files missing or corrupted. Check logs above.")
        
        return "\n".join(results)
        
    except Exception as e:
        return f"❌ Validation failed: {str(e)}"


@udf(returnType=StringType())
def performance_test_udf(catalog, bronze_db):
    """
    Performance UDF: Tests read/write performance on the extracted files.
    
    Args:
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        str: Performance test results
    """
    try:
        base_path = f"/Volumes/{catalog}/{bronze_db}/payer/files"
        results = []
        results.append("=== PERFORMANCE TEST REPORT ===")
        
        test_files = [
            ("claims", "claims.csv"),
            ("members", "members.csv")  # Test subset for performance
        ]
        
        for folder, filename in test_files:
            file_path = f"{base_path}/{folder}/{filename}"
            
            if os.path.exists(file_path):
                # Test read performance
                start_time = time.time()
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    line_count = 0
                    char_count = 0
                    for line in f:
                        line_count += 1
                        char_count += len(line)
                
                read_time = time.time() - start_time
                
                results.append(f"📊 {folder}/{filename}:")
                results.append(f"   ⏱️ Read Time: {read_time:.3f} seconds")
                results.append(f"   📝 Lines Read: {line_count:,}")
                results.append(f"   📏 Characters: {char_count:,}")
                results.append(f"   🚀 Throughput: {line_count/read_time:.0f} lines/sec")
            else:
                results.append(f"❌ {folder}/{filename}: File not found for performance test")
        
        return "\n".join(results)
        
    except Exception as e:
        return f"❌ Performance test failed: {str(e)}"


@udf(returnType=StringType())
def data_quality_check_udf(catalog, bronze_db):
    """
    Data Quality UDF: Performs basic data quality checks on extracted files.
    
    Args:
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        str: Data quality report
    """
    try:
        base_path = f"/Volumes/{catalog}/{bronze_db}/payer/files"
        results = []
        results.append("=== DATA QUALITY REPORT ===")
        
        quality_checks = {
            ("claims", "claims.csv"): ["claim_id", "member_id", "total_charge"],
            ("members", "members.csv"): ["member_id", "first_name", "last_name"],
            ("providers", "providers.csv"): ["provider_id", "provider_name"]
        }
        
        for (folder, filename), expected_columns in quality_checks.items():
            file_path = f"{base_path}/{folder}/{filename}"
            
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        header = f.readline().strip()
                        actual_columns = [col.strip() for col in header.split(',')]
                        
                        # Check for expected columns
                        missing_columns = [col for col in expected_columns if col not in actual_columns]
                        extra_columns = [col for col in actual_columns if col not in expected_columns]
                        
                        results.append(f"📋 {folder}/{filename}:")
                        results.append(f"   📝 Total Columns: {len(actual_columns)}")
                        results.append(f"   ✅ Expected Columns Found: {len(expected_columns) - len(missing_columns)}/{len(expected_columns)}")
                        
                        if missing_columns:
                            results.append(f"   ❌ Missing Columns: {missing_columns}")
                        if extra_columns:
                            results.append(f"   ➕ Extra Columns: {extra_columns}")
                        
                        # Sample first data row
                        first_data_row = f.readline().strip()
                        if first_data_row:
                            results.append(f"   📄 Sample Row: {first_data_row[:100]}...")
                        
                except Exception as read_error:
                    results.append(f"⚠️ {folder}/{filename}: Quality check failed - {str(read_error)}")
            else:
                results.append(f"❌ {folder}/{filename}: File not found for quality check")
        
        return "\n".join(results)
        
    except Exception as e:
        return f"❌ Data quality check failed: {str(e)}"


def run_all_tests(spark, catalog, bronze_db):
    """
    Run all validation tests on extracted files.
    
    Args:
        spark: Spark session
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        dict: Results of all tests
    """
    print("🔍 Running comprehensive test suite on extracted files...")
    print("=" * 60)
    
    # Create test DataFrame for UDF execution
    test_df = spark.createDataFrame([(catalog, bronze_db)], ["catalog", "bronze_db"])
    
    results = {}
    
    # Test 1: File Validation
    print("1️⃣ RUNNING FILE VALIDATION TEST...")
    validation_result = test_df.select(
        validate_extracted_files_udf("catalog", "bronze_db").alias("validation")
    ).collect()[0]["validation"]
    print(validation_result)
    print("\n" + "=" * 60)
    results['validation'] = validation_result
    
    # Test 2: Performance Testing
    print("2️⃣ RUNNING PERFORMANCE TEST...")
    performance_result = test_df.select(
        performance_test_udf("catalog", "bronze_db").alias("performance")
    ).collect()[0]["performance"]
    print(performance_result)
    print("\n" + "=" * 60)
    results['performance'] = performance_result
    
    # Test 3: Data Quality Check
    print("3️⃣ RUNNING DATA QUALITY CHECK...")
    quality_result = test_df.select(
        data_quality_check_udf("catalog", "bronze_db").alias("quality")
    ).collect()[0]["quality"]
    print(quality_result)
    print("\n" + "=" * 60)
    results['quality'] = quality_result
    
    # Summary
    print("🎯 TEST SUITE COMPLETED!")
    print("All tests have been executed. Review the results above to ensure:")
    print("✅ All files were extracted successfully")
    print("✅ File sizes and row counts are reasonable") 
    print("✅ Performance is acceptable")
    print("✅ Data quality meets expectations")
    
    # Determine overall success
    all_success = all("❌" not in result for result in results.values())
    results['overall_success'] = all_success
    
    if all_success:
        print("\n🎉 All tests passed! Files are ready for the data pipeline.")
    else:
        print("\n⚠️ Some tests failed. Check individual test results above.")
    
    return results


def quick_health_check(spark, catalog, bronze_db):
    """
    Quick health check: Fast verification without UDF overhead.
    
    Args:
        spark: Spark session  
        catalog (str): Unity Catalog name
        bronze_db (str): Bronze database/schema name
    
    Returns:
        dict: Quick health check results
    """
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        print("🏥 QUICK HEALTH CHECK")
        print("=" * 40)
        
        results = {
            'volume_accessible': False,
            'folders_found': 0,
            'total_folders': 5,
            'errors': []
        }
        
        # Use dbutils to check (works from driver)
        try:
            files = dbutils.fs.ls(f"/Volumes/{catalog}/{bronze_db}/payer/files/")
            print(f"✅ Volume accessible: {len(files)} top-level items found")
            results['volume_accessible'] = True
            
            # Check each expected folder
            expected_folders = ["claims", "diagnosis", "procedures", "members", "providers"]
            found_folders = 0
            
            for folder in expected_folders:
                try:
                    folder_files = dbutils.fs.ls(f"/Volumes/{catalog}/{bronze_db}/payer/files/{folder}/")
                    if folder_files:
                        print(f"✅ {folder}: {len(folder_files)} file(s)")
                        found_folders += 1
                    else:
                        print(f"⚠️ {folder}: Empty")
                except Exception as e:
                    print(f"❌ {folder}: Not found")
                    results['errors'].append(f"{folder}: {str(e)}")
            
            results['folders_found'] = found_folders
            print(f"\n📊 Summary: {found_folders}/{len(expected_folders)} folders found")
            
            if found_folders == len(expected_folders):
                print("🎉 All expected folders present!")
            else:
                print("⚠️ Some folders missing. Consider re-running extraction.")
                
        except Exception as e:
            print(f"❌ Volume not accessible: {str(e)}")
            print("💡 Try running the extraction first")
            results['errors'].append(f"Volume access error: {str(e)}")
        
        results['success'] = results['volume_accessible'] and results['folders_found'] == results['total_folders']
        return results
        
    except Exception as e:
        print(f"❌ Health check failed: {str(e)}")
        return {'success': False, 'errors': [str(e)]}
