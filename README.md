<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/tum-gis/sensordb/main/assets/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/tum-gis/sensordb/main/assets/logo-light.svg">
    <img alt="3DSensorDB - A geospatial database for storing, managing, and analyzing 3D sensor data."
         src="https://raw.githubusercontent.com/tum-gis/sensordb/main/assets/logo-light.svg"
         width="50%">
  </picture>
</div>

## About

[3DSensorDB](https://sensordb.org) is a geospatial database for storing, managing, and analyzing 3D sensor data.
When combined with semantic 3D environment models in CityGML, sensor observations can be linked and enriched with semantic,
topological, geometric, and appearance information.

The system is built on [PostgreSQL](https://www.postgresql.org/)/[PostGIS](https://postgis.net/), the [3D City Database](https://github.com/3dcitydb/3dcitydb) supporting [CityGML 1.0–3.0](https://www.ogc.org/standards/citygml/), and [Rust](https://www.rust-lang.org/) for blazingly fast processing.

>  This is an early version of the software and still has a few rough edges.
> If you are interested in using the software, extending it for your use case, or in the sharded version for processing massive sensor data amounts, please contact benedikt.schwab@tum.de.

## Getting Started

For a quick start, check out the [documentation](https://sensordb.org/docs) for detailed instructions.

## Citation

If you use *3DSensorDB* in your research, please cite the [following preprint](https://arxiv.org/abs/2603.11252):

```bibtex
@misc{schwab2026radiometricfingerprinting,
      title={Radiometric fingerprinting of object surfaces using mobile laser scanning and semantic 3D road space models},
      author={Benedikt Schwab and Thomas H. Kolbe},
      year={2026},
      eprint={2603.11252},
      archivePrefix={arXiv},
      primaryClass={cs.CV},
      url={https://arxiv.org/abs/2603.11252},
}
```

The semantic 3D road space models used in this study are publicly available as open data in [this repository](https://github.com/savenow/lod3-road-space-models).

### Acknowledgements

Sincere thanks to the [development partners](https://docs.3dcitydb.org/latest/contributors/) of the [3D City Database](https://www.3dcitydb.org/), 
which serves as both a reference for this project and enables linking 3D sensor data to semantic models in CityGML.
