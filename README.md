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

If you use *3DSensorDB* in your research, please cite the [following article](https://doi.org/10.1016/j.jag.2026.105533):

```bibtex
@article{schwabRadiometricFingerprinting2026,
    author = {Benedikt Schwab and Thomas H. Kolbe},
    title = {Radiometric fingerprinting of object surfaces using mobile laser scanning and semantic 3D road space models},
    journal = {International Journal of Applied Earth Observation and Geoinformation},
    volume = {153},
    pages = {105533},
    year = {2026},
    issn = {1569-8432},
    doi = {https://doi.org/10.1016/j.jag.2026.105533},
    url = {https://www.sciencedirect.com/science/article/pii/S1569843226004498},
    keywords = {Radiometric fingerprint, Mobile laser scanning, Semantic road space model, CityGML, 3DSensorDB},
    abstract = {Although semantic 3D city models are internationally available and becoming increasingly detailed, the incorporation of material information remains largely untapped. However, a structured representation of materials and their physical properties could substantially broaden the application spectrum and analytical capabilities for urban digital twins. At the same time, the growing number of repeated mobile laser scans of cities and their street spaces yields a wealth of observations influenced by the material characteristics of the corresponding surfaces. To leverage this information, we propose radiometric fingerprints of object surfaces by grouping LiDAR observations reflected from the same semantic object under varying distances, incidence angles, environmental conditions, sensors, and scanning campaigns. Our study demonstrates how 312.4million individual beams acquired across four campaigns using five LiDAR sensors on the Audi Autonomous Driving Dataset (A2D2) vehicle can be automatically associated with 6368 individual objects of the semantic 3D city model. The model comprises a comprehensive and semantic representation of four inner-city streets at Level of Detail (LOD) 3 with centimeter-level accuracy. It is based on the CityGML 3.0 standard and enables fine-grained sub-differentiation of objects. The extracted radiometric fingerprints for object surfaces reveal recurring intra-class patterns that indicate class-dominant materials. The semantic model, the method implementations, and the developed geodatabase solution 3DSensorDB are released under: https://github.com/tum-gis/sensordb}
}
```

The semantic 3D road space models used in this study are publicly available as open data in [this repository](https://github.com/savenow/lod3-road-space-models).

### Acknowledgements

Sincere thanks to the [development partners](https://docs.3dcitydb.org/latest/contributors/) of the [3D City Database](https://www.3dcitydb.org/), 
which serves as both a reference for this project and enables linking 3D sensor data to semantic models in CityGML.
