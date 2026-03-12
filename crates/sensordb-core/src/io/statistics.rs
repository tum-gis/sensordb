use crate::database::datatype::{
    CampaignId, FeatureClassName, FeatureId, FeatureObjectName, SensorId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticsDocument {
    pub overview: Overview,
    pub city_model_stats: CityModelStatistics,
    pub sensor_stats: Option<SensorStatistics>,
}

impl StatisticsDocument {
    pub fn new(
        overview: Overview,
        city_model_stats: CityModelStatistics,
        sensor_stats: Option<SensorStatistics>,
    ) -> Self {
        Self {
            overview,
            city_model_stats,
            sensor_stats,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Overview {
    pub sensors: BTreeMap<SensorId, String>,
    pub campaigns: BTreeMap<CampaignId, String>,
    pub feature_class_names: Vec<FeatureClassName>,
    //pub feature_object_ids: BTreeMap<FeatureId, String>,
}

impl Overview {
    pub fn new(
        sensors: BTreeMap<SensorId, String>,
        campaigns: BTreeMap<CampaignId, String>,
        feature_class_names: Vec<FeatureClassName>,
        _feature_object_ids: BTreeMap<FeatureId, String>,
    ) -> Self {
        Self {
            sensors,
            campaigns,
            feature_class_names,
            //feature_object_ids,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CityModelStatistics {
    pub number_of_features: u64,
    pub group_feature_class_name:
        BTreeMap<FeatureClassName, CityModelFeatureClassNameGroupStatistics>,
}

impl CityModelStatistics {
    pub fn new(
        number_of_features: u64,
        group_feature_class_name: BTreeMap<
            FeatureClassName,
            CityModelFeatureClassNameGroupStatistics,
        >,
    ) -> Self {
        Self {
            number_of_features,
            group_feature_class_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CityModelFeatureClassNameGroupStatistics {
    pub number_of_features: u64,
    pub group_feature_object_name:
        BTreeMap<FeatureObjectName, CityModelFeatureClassNameFeatureObjectNameGroupStatistics>,
}

impl CityModelFeatureClassNameGroupStatistics {
    pub fn new(
        number_of_features: u64,
        group_feature_object_name: BTreeMap<
            FeatureObjectName,
            CityModelFeatureClassNameFeatureObjectNameGroupStatistics,
        >,
    ) -> Self {
        Self {
            number_of_features,
            group_feature_object_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CityModelFeatureClassNameFeatureObjectNameGroupStatistics {
    pub number_of_features: u64,
}

impl CityModelFeatureClassNameFeatureObjectNameGroupStatistics {
    pub fn new(number_of_features: u64) -> Self {
        Self { number_of_features }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorStatistics {
    pub number_of_associated_features: u64,
    pub group_sensor: BTreeMap<SensorId, SensorGroupedStatistics>,
    pub group_campaign: BTreeMap<CampaignId, CampaignGroupedStatistics>,
    pub group_feature_class_name: BTreeMap<FeatureClassName, FeatureClassNameGroupedStatistics>,
}

impl SensorStatistics {
    pub fn new(
        number_of_associated_features: u64,
        group_sensor: BTreeMap<SensorId, SensorGroupedStatistics>,
        group_campaign: BTreeMap<CampaignId, CampaignGroupedStatistics>,
        group_feature_class_name: BTreeMap<FeatureClassName, FeatureClassNameGroupedStatistics>,
    ) -> Self {
        Self {
            number_of_associated_features,
            group_sensor,
            group_campaign,
            group_feature_class_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorGroupedStatistics {
    pub number_of_points: u64,
    pub number_of_associated_points: u64,
    pub number_of_associated_features: u64,
}

impl SensorGroupedStatistics {
    pub fn new(
        number_of_points: u64,
        number_of_associated_points: u64,
        number_of_associated_features: u64,
    ) -> Self {
        Self {
            number_of_points,
            number_of_associated_points,
            number_of_associated_features,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignGroupedStatistics {
    pub number_of_points: u64,
    pub number_of_associated_points: u64,
    pub number_of_associated_features: u64,
    pub group_spherical_range: BTreeMap<u32, CampaignSphericalRangeGroupedStatistics>,
}

impl CampaignGroupedStatistics {
    pub fn new(
        number_of_points: u64,
        number_of_associated_points: u64,
        number_of_associated_features: u64,
        group_spherical_range: BTreeMap<u32, CampaignSphericalRangeGroupedStatistics>,
    ) -> Self {
        Self {
            number_of_points,
            number_of_associated_points,
            number_of_associated_features,
            group_spherical_range,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CampaignSphericalRangeGroupedStatistics {
    pub spherical_range_bin_min: f64,
    pub spherical_range_bin_max: f64,
    pub number_of_points: u64,
    pub number_of_associated_points: u64,
    pub number_of_associated_features: u64,
}

impl CampaignSphericalRangeGroupedStatistics {
    pub fn new(
        spherical_range_bin_min: f64,
        spherical_range_bin_max: f64,
        number_of_points: u64,
        number_of_associated_points: u64,
        number_of_associated_features: u64,
    ) -> Self {
        Self {
            spherical_range_bin_min,
            spherical_range_bin_max,
            number_of_points,
            number_of_associated_points,
            number_of_associated_features,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureClassNameGroupedStatistics {
    pub number_of_features: u64,
    pub number_of_associated_points: u64,
    pub group_feature_object_name: BTreeMap<FeatureObjectName, FeatureObjectNameGroupedStatistics>,
    pub group_campaign_sensor:
        BTreeMap<CampaignId, BTreeMap<SensorId, AssociatedPointCloudStatistics>>,
    pub group_feature_object_name_campaign_sensor: BTreeMap<
        FeatureObjectName,
        BTreeMap<CampaignId, BTreeMap<SensorId, AssociatedPointCloudStatistics>>,
    >,
}

impl FeatureClassNameGroupedStatistics {
    pub fn new(
        number_of_features: u64,
        number_of_associated_points: u64,
        group_feature_object_name: BTreeMap<FeatureObjectName, FeatureObjectNameGroupedStatistics>,
        group_campaign_sensor: BTreeMap<
            CampaignId,
            BTreeMap<SensorId, AssociatedPointCloudStatistics>,
        >,
        group_feature_object_name_campaign_sensor: BTreeMap<
            FeatureObjectName,
            BTreeMap<CampaignId, BTreeMap<SensorId, AssociatedPointCloudStatistics>>,
        >,
    ) -> Self {
        Self {
            number_of_features,
            number_of_associated_points,
            group_feature_object_name,
            group_campaign_sensor,
            group_feature_object_name_campaign_sensor,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureObjectNameGroupedStatistics {
    pub number_of_features: u64,
    pub number_of_associated_points: u64,
}

impl FeatureObjectNameGroupedStatistics {
    pub fn new(number_of_features: u64, number_of_associated_points: u64) -> Self {
        Self {
            number_of_features,
            number_of_associated_points,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssociatedPointCloudStatistics {
    pub number_of_associated_points: u64,
    pub number_of_associated_features: u64,
}

impl AssociatedPointCloudStatistics {
    pub fn new(number_of_associated_points: u64, number_of_associated_features: u64) -> Self {
        Self {
            number_of_associated_points,
            number_of_associated_features,
        }
    }
}
