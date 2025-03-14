import pandas as pd 

def transform_qs_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    cols = {
        'SampleID': 'id_repository',
        'Lane': 'lane',
        'index': 'index',
        'index2': 'index2',
        'ReadNumber': 'read_number',
        'Yield': 'yield',
        'YieldQ30': 'yield_q30',
        'QualityScoreSum': 'quality_score_sum',
        'Mean Quality Score (PF)': 'mean_quality_score_pf',
        '% Q30': 'percent_q30'
    }
    df.rename(columns=cols, inplace=True)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    df = df.astype(str)
    
    # Need to fillna so that the mysql connector can insert the data.
    df.fillna(value="", inplace=True)
    new_cols = list(cols.values()) + ["created_at", "updated_at"]
    df = df[new_cols]

    return df

def transform_qc_data(df: pd.DataFrame, ts: str) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates()

    df['id_repository'] = df['Sample'].str.split('|').apply(lambda x: x[-1].strip())  
    df['run_name'] = df['Sample'].str.split('|').apply(lambda x: x[-3].strip().strip().split('-')[0]) 
    
    cols = {
        'id_repository': 'id_repository',
        'run_name':'run_name',
        'dragen_mapping-Number_of_duplicate_marked_reads_pct': 'percent_dups',
        'dragen_mapping-Q30_bases_pct': 'percent_q30_bases', 
        'dragen_mapping-Total_input_reads': 'total_seqs',
        # 'dragen_mapping-Reads_with_mate_sequenced_pct': 'm_seqs',
        'dragen_mapping-Estimated_sample_contamination': 'contam',
        'dragen_mapping-Secondary_alignments_pct': 'non_primary',
        'dragen_mapping-Mapped_reads_pct': 'percent_mapped',
        'dragen_mapping-Properly_paired_reads_pct': 'percent_proper_pairs',
        'dragen_mapping-Total_alignments': 'reads_mapped',
        'dragen_coverage-wgs_pct_of_genome_with_coverage_50x_inf': 'at_least_50x',
        # 'dragen_coverage-wgs_pct_of_genome_with_coverage_30x_inf': 'at_least_30x',
        'dragen_coverage-wgs_pct_of_genome_with_coverage_20x_inf': 'at_least_20x',
        'dragen_coverage-wgs_pct_of_genome_with_coverage_10x_inf': 'at_least_10x',
        'dragen_coverage-wgs_median_autosomal_coverage_over_genome': 'median_coverage',
        'dragen_variant_calling-Total': 'vars',
        'dragen_variant_calling-SNPs_pct': 'snp',
        'dragen_variant_calling-Indels_pct': 'indel',
        'dragen_variant_calling-Ti_Tv_ratio': 'ts_tv',
        'dragen_coverage-wgs_average_alignment_coverage_over_genome': 'depth',
        'dragen_ploidy_estimation-Ploidy_estimation': 'ploidy_estimation'
    }
    df.rename(columns=cols, inplace=True)

    if "created_at" not in df.columns:
        df["created_at"] = ts
    if "updated_at" not in df.columns:
        df["updated_at"] = ts

    # Need to fillna so that the mysql connector can insert the data.
    df.fillna(value="", inplace=True)
    df = df[["id_repository","percent_dups","percent_q30_bases","total_seqs","contam","non_primary","percent_mapped","percent_proper_pairs","reads_mapped","at_least_50x","at_least_20x","at_least_10x","median_coverage","vars","snp","indel","ts_tv","depth","ploidy_estimation","created_at","updated_at","run_name"]]

    df = df.astype(str)
    return df