pub mod algorithm;
pub mod analyze_account_name_similarity;
pub mod group_account_name_by_similarity;

pub use crate::analyze_account_name_similarity::{CAccountNameSimAnalyse,
                                                 CAccountNameSimAnalyseParamsWeightTable,
                                                 CAccountNameSimResultDetail};

pub use crate::group_account_name_by_similarity::{CAccountNameAnaVec,
                                                  CSimilarityGroupingThreshold,
                                                  EfficiencyMode};


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo() {
        // 计算两个账号名的相似度    Calculate the similarity of two account names
        let sim = CAccountNameSimAnalyse::new("0ubutz22ae22").calc_similarity(&CAccountNameSimAnalyse::new("2ubutz10ae57"));
        assert_eq!(sim.0, 0.7883572886890006);
        // assert_eq!(sim.1, CAccountNameSimResultDetail {
        //                    sim_total_score: 0.7883572886890006,
        //                    sim_score: 0.9470814482092683,
        //                    sim_jaro_distance: 0.7222222222222223,
        //                    sim_edit_distance: 5,
        //                    sim_length: 1.0,
        //                    sim_item_list: 0.8147850687324387,
        //                    sim_item_amount: 1.0,
        //                    sim_skeleton_style: 1.0,
        //                    sim_skeleton_part_size_list: 1.0 });
        println!("{:?}", CAccountNameSimAnalyse::new("0ubutz22ae22").calc_similarity(&CAccountNameSimAnalyse::new("2ubutz10ae57")));
    }
}



