import pandas as pd
import re

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
    def _get_run_name(row):
        temp_run_name = row.split("|")[-3].strip()
        pattern_list = [
            "(.*)(?:-DRAGEN_Germline_WGS_4-2-7.*)", # for cases like 0C0194101C05_fe14319c_rerun_2025-02-26_043012 which comes from 0H0155101C01_5d61bff4_rerun_2024-08-13_013035-DRAGEN_Germline_WGS_4-2-7_sw-mode-JK-31f0798c-f818-4127-b51f-9bb766a396ad
            "(.*)(?:-DRAGEN_Germline_WGS_4-2-6-v2.*)", # for cases like 10002030401-1-DRAGEN-4-2-6-Germline-All-Callers which comes from  0G0021201C01-1-DRAGEN-4-2-6-Germline-All-Callers-DRAGEN_Germline_WGS_4-2-6-v2_sw-mode-JK-5b9ca3ee-490d-4e8b-b5ad-d084acfbe819
            "([A-Z0-9]{12}_[a-zA-Z0-9]{8}_rerun_[\d]{4}\-[\d]{2}-[\d]{2}_[\d]{1,})(?:\-[0-9a-f]{8}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{4}\-[0-9a-f]{12})" # for specific case of 0C0194101C05_fe14319c_rerun_2025-02-26_043012 from "/ | tmp | nxf.eOUZXPFKdi | local_files | 0C0194101C05_fe14319c_rerun_2025-02-26_043012-2dd6b9ea-9e2b-4a69-8b54-6a4980054643 | 0C0194101C05 | 0C0194101C05"
        ]
        for pattern in pattern_list:
            if re.match(pattern, temp_run_name):
                return re.findall(pattern, temp_run_name)[0]
        return temp_run_name.split('-')[0]

    df['run_name'] = df['Sample'].apply(_get_run_name)
    
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