"""
Bias Detection Utilities for Healthcare Analytics
================================================

This module provides utilities for detecting and analyzing various types of bias
in healthcare data, specifically designed for actuarial continuing education
and healthcare analytics applications.

Author: Healthcare Analytics Team
Date: 2025
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, count, sum as spark_sum, avg, stddev, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats


class BiasDetector:
    """
    A comprehensive bias detection class for healthcare data analysis.
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        
    def detect_statistical_bias(self, df: DataFrame, target_column: str, 
                              group_columns: List[str] = None) -> Dict[str, Any]:
        """
        Detect statistical biases in the dataset.
        
        Args:
            df: Spark DataFrame to analyze
            target_column: Column to analyze for bias
            group_columns: Columns to group by for bias analysis
            
        Returns:
            Dictionary containing bias analysis results
        """
        results = {}
        
        # Survivorship bias detection
        results['survivorship_bias'] = self._detect_survivorship_bias(df, target_column)
        
        # Selection bias detection
        results['selection_bias'] = self._detect_selection_bias(df, target_column, group_columns)
        
        # Data bias detection
        results['data_bias'] = self._detect_data_bias(df, target_column)
        
        return results
    
    def detect_cognitive_bias(self, df: DataFrame, decision_columns: List[str]) -> Dict[str, Any]:
        """
        Detect cognitive biases in decision-making data.
        
        Args:
            df: Spark DataFrame to analyze
            decision_columns: Columns representing decision variables
            
        Returns:
            Dictionary containing cognitive bias analysis
        """
        results = {}
        
        # Anchoring bias detection
        results['anchoring_bias'] = self._detect_anchoring_bias(df, decision_columns)
        
        # Confirmation bias detection
        results['confirmation_bias'] = self._detect_confirmation_bias(df, decision_columns)
        
        return results
    
    def detect_social_bias(self, df: DataFrame, demographic_columns: List[str], 
                          target_column: str) -> Dict[str, Any]:
        """
        Detect social biases in healthcare data.
        
        Args:
            df: Spark DataFrame to analyze
            demographic_columns: Columns representing demographic information
            target_column: Target variable to analyze for bias
            
        Returns:
            Dictionary containing social bias analysis
        """
        results = {}
        
        # Racial bias detection
        results['racial_bias'] = self._detect_racial_bias(df, demographic_columns, target_column)
        
        # Gender bias detection
        results['gender_bias'] = self._detect_gender_bias(df, demographic_columns, target_column)
        
        # Age bias detection
        results['age_bias'] = self._detect_age_bias(df, demographic_columns, target_column)
        
        return results
    
    def detect_modeling_bias(self, predictions: DataFrame, actual: DataFrame, 
                           demographic_columns: List[str]) -> Dict[str, Any]:
        """
        Detect bias in machine learning model predictions.
        
        Args:
            predictions: DataFrame with model predictions
            actual: DataFrame with actual values
            demographic_columns: Columns representing demographic information
            
        Returns:
            Dictionary containing modeling bias analysis
        """
        results = {}
        
        # Fairness metrics
        results['fairness_metrics'] = self._calculate_fairness_metrics(predictions, actual, demographic_columns)
        
        # Disparate impact analysis
        results['disparate_impact'] = self._analyze_disparate_impact(predictions, demographic_columns)
        
        return results
    
    def _detect_survivorship_bias(self, df: DataFrame, target_column: str) -> Dict[str, Any]:
        """Detect survivorship bias in the dataset."""
        # Check for missing data patterns that might indicate survivorship bias
        missing_data = df.select(
            count(when(isnull(target_column), 1)).alias("missing_count"),
            count("*").alias("total_count")
        ).collect()[0]
        
        missing_rate = missing_data["missing_count"] / missing_data["total_count"]
        
        return {
            "missing_rate": missing_rate,
            "bias_indicator": "High" if missing_rate > 0.1 else "Low",
            "description": "Survivorship bias occurs when only 'surviving' data points are included"
        }
    
    def _detect_selection_bias(self, df: DataFrame, target_column: str, 
                              group_columns: List[str]) -> Dict[str, Any]:
        """Detect selection bias in the dataset."""
        if not group_columns:
            return {"error": "Group columns required for selection bias analysis"}
        
        # Analyze distribution across groups
        group_stats = df.groupBy(*group_columns).agg(
            count("*").alias("count"),
            avg(target_column).alias("mean"),
            stddev(target_column).alias("std")
        ).collect()
        
        # Calculate coefficient of variation across groups
        means = [row["mean"] for row in group_stats if row["mean"] is not None]
        if len(means) > 1:
            cv = np.std(means) / np.mean(means)
        else:
            cv = 0
        
        return {
            "coefficient_of_variation": cv,
            "bias_indicator": "High" if cv > 0.3 else "Low",
            "group_statistics": group_stats,
            "description": "Selection bias occurs when certain groups are over/under-represented"
        }
    
    def _detect_data_bias(self, df: DataFrame, target_column: str) -> Dict[str, Any]:
        """Detect data bias in the dataset."""
        # Check for outliers and data quality issues
        stats = df.select(
            avg(target_column).alias("mean"),
            stddev(target_column).alias("std"),
            min(target_column).alias("min"),
            max(target_column).alias("max")
        ).collect()[0]
        
        # Calculate outlier rate (using IQR method)
        q1 = df.selectExpr(f"percentile_approx({target_column}, 0.25)").collect()[0][0]
        q3 = df.selectExpr(f"percentile_approx({target_column}, 0.75)").collect()[0][0]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outlier_count = df.filter(
            (col(target_column) < lower_bound) | (col(target_column) > upper_bound)
        ).count()
        
        total_count = df.count()
        outlier_rate = outlier_count / total_count if total_count > 0 else 0
        
        return {
            "outlier_rate": outlier_rate,
            "bias_indicator": "High" if outlier_rate > 0.05 else "Low",
            "statistics": stats,
            "description": "Data bias occurs when data quality issues affect analysis"
        }
    
    def _detect_anchoring_bias(self, df: DataFrame, decision_columns: List[str]) -> Dict[str, Any]:
        """Detect anchoring bias in decision-making."""
        # Look for patterns where decisions cluster around certain values
        results = {}
        
        for col_name in decision_columns:
            # Calculate if values cluster around specific points
            stats = df.select(
                avg(col_name).alias("mean"),
                stddev(col_name).alias("std")
            ).collect()[0]
            
            # Check for clustering (low standard deviation relative to mean)
            if stats["mean"] and stats["std"]:
                cv = stats["std"] / abs(stats["mean"])
                results[col_name] = {
                    "coefficient_of_variation": cv,
                    "bias_indicator": "High" if cv < 0.1 else "Low",
                    "description": "Anchoring bias occurs when decisions cluster around initial values"
                }
        
        return results
    
    def _detect_confirmation_bias(self, df: DataFrame, decision_columns: List[str]) -> Dict[str, Any]:
        """Detect confirmation bias in decision-making."""
        # Look for patterns where decisions consistently favor certain outcomes
        results = {}
        
        for col_name in decision_columns:
            # Calculate distribution skewness
            skewness = df.selectExpr(f"skewness({col_name})").collect()[0][0]
            
            results[col_name] = {
                "skewness": skewness,
                "bias_indicator": "High" if abs(skewness) > 1 else "Low",
                "description": "Confirmation bias occurs when decisions consistently favor certain outcomes"
            }
        
        return results
    
    def _detect_racial_bias(self, df: DataFrame, demographic_columns: List[str], 
                           target_column: str) -> Dict[str, Any]:
        """Detect racial bias in healthcare data."""
        # This is a simplified example - in practice, you'd need proper demographic data
        results = {}
        
        # Look for demographic columns that might indicate race/ethnicity
        race_columns = [col for col in demographic_columns if any(
            term in col.lower() for term in ['race', 'ethnicity', 'ethnic']
        )]
        
        if race_columns:
            for race_col in race_columns:
                # Calculate mean target by race group
                race_stats = df.groupBy(race_col).agg(
                    avg(target_column).alias("mean_target"),
                    count("*").alias("count")
                ).collect()
                
                results[race_col] = {
                    "group_statistics": race_stats,
                    "bias_indicator": "Requires detailed analysis",
                    "description": "Racial bias occurs when outcomes differ systematically by race"
                }
        else:
            results["note"] = "No race/ethnicity columns found for analysis"
        
        return results
    
    def _detect_gender_bias(self, df: DataFrame, demographic_columns: List[str], 
                           target_column: str) -> Dict[str, Any]:
        """Detect gender bias in healthcare data."""
        results = {}
        
        # Look for gender columns
        gender_columns = [col for col in demographic_columns if any(
            term in col.lower() for term in ['gender', 'sex', 'male', 'female']
        )]
        
        if gender_columns:
            for gender_col in gender_columns:
                # Calculate mean target by gender
                gender_stats = df.groupBy(gender_col).agg(
                    avg(target_column).alias("mean_target"),
                    count("*").alias("count")
                ).collect()
                
                results[gender_col] = {
                    "group_statistics": gender_stats,
                    "bias_indicator": "Requires detailed analysis",
                    "description": "Gender bias occurs when outcomes differ systematically by gender"
                }
        else:
            results["note"] = "No gender columns found for analysis"
        
        return results
    
    def _detect_age_bias(self, df: DataFrame, demographic_columns: List[str], 
                        target_column: str) -> Dict[str, Any]:
        """Detect age bias in healthcare data."""
        results = {}
        
        # Look for age columns
        age_columns = [col for col in demographic_columns if any(
            term in col.lower() for term in ['age', 'birth', 'year']
        )]
        
        if age_columns:
            for age_col in age_columns:
                # Calculate correlation between age and target
                correlation = df.selectExpr(f"corr({age_col}, {target_column})").collect()[0][0]
                
                results[age_col] = {
                    "correlation": correlation,
                    "bias_indicator": "High" if abs(correlation) > 0.7 else "Low",
                    "description": "Age bias occurs when outcomes are strongly correlated with age"
                }
        else:
            results["note"] = "No age columns found for analysis"
        
        return results
    
    def _calculate_fairness_metrics(self, predictions: DataFrame, actual: DataFrame, 
                                  demographic_columns: List[str]) -> Dict[str, Any]:
        """Calculate fairness metrics for model predictions."""
        # This is a simplified implementation
        # In practice, you'd implement comprehensive fairness metrics
        return {
            "demographic_parity": "Not implemented - requires detailed demographic data",
            "equalized_odds": "Not implemented - requires detailed demographic data",
            "description": "Fairness metrics measure whether model predictions are fair across demographic groups"
        }
    
    def _analyze_disparate_impact(self, predictions: DataFrame, 
                                 demographic_columns: List[str]) -> Dict[str, Any]:
        """Analyze disparate impact in model predictions."""
        # This is a simplified implementation
        return {
            "impact_ratio": "Not implemented - requires detailed demographic data",
            "description": "Disparate impact analysis measures whether model decisions disproportionately affect certain groups"
        }


def create_bias_report(bias_results: Dict[str, Any]) -> str:
    """
    Create a comprehensive bias analysis report.
    
    Args:
        bias_results: Results from bias detection analysis
        
    Returns:
        Formatted report string
    """
    report = "# Bias Analysis Report\n\n"
    
    for bias_type, results in bias_results.items():
        report += f"## {bias_type.replace('_', ' ').title()}\n\n"
        
        if isinstance(results, dict):
            for key, value in results.items():
                if isinstance(value, dict):
                    report += f"### {key.replace('_', ' ').title()}\n"
                    for sub_key, sub_value in value.items():
                        report += f"- **{sub_key.replace('_', ' ').title()}**: {sub_value}\n"
                    report += "\n"
                else:
                    report += f"- **{key.replace('_', ' ').title()}**: {value}\n"
        else:
            report += f"{results}\n"
        
        report += "\n"
    
    return report


def visualize_bias_analysis(bias_results: Dict[str, Any], save_path: Optional[str] = None):
    """
    Create visualizations for bias analysis results.
    
    Args:
        bias_results: Results from bias detection analysis
        save_path: Optional path to save visualizations
    """
    # This is a placeholder for visualization functionality
    # In practice, you'd create specific visualizations based on the bias results
    print("Bias analysis visualizations would be created here")
    print("Consider creating plots for:")
    print("- Distribution comparisons across demographic groups")
    print("- Bias indicator summaries")
    print("- Fairness metric visualizations")

