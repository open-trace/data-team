> **Snapshot note:** Regenerated from `opentrace-prod-5ga4.mart_dev` (2026-09-05). See [MART_QA_NOTES.md](./MART_QA_NOTES.md).

## dim_disease

### disease_key (STRING, stats only, distinct=12, min=02422165190d7d1400a7075f707dd945, max=dfa16d993c5cc8ed35215f13552d0c2d)

### disease_or_hazard (STRING, distinct=12)
| label_value | n | pct |
|---|---:|---:|
| `EIEC` | 1 | 8.3 |
| `Aspergillus spp` | 1 | 8.3 |
| `E.coli` | 1 | 8.3 |
| `EPEC ` | 1 | 8.3 |
| `Staphylococus ` | 1 | 8.3 |
| `ETEC ` | 1 | 8.3 |
| `EAEC ` | 1 | 8.3 |
| `STEC ` | 1 | 8.3 |
| `salmonella` | 1 | 8.3 |
| `Fusarium spp` | 1 | 8.3 |
| `Mycotoxins` | 1 | 8.3 |
| `Penicillium spp` | 1 | 8.3 |

### disease_family (STRING, distinct=1)
| label_value | n | pct |
|---|---:|---:|
| `foodborne` | 12 | 100.0 |

### loaded_at (TIMESTAMP, stats only, distinct=1, min=2026-08-24 14:22:54.196250+00, max=2026-08-24 14:22:54.196250+00)

## dim_land_use

### land_use_key (STRING, stats only, distinct=23, min=182c5d6bfa7830cadbae251d44af8cb2, max=fd21a7899e6e52e3d2484dc89c8e01f4)

### land_use_code (STRING, distinct=23)
| label_value | n | pct |
|---|---:|---:|
| `cropland area under organic agric.` | 1 | 4.3 |
| `temporary fallow` | 1 | 4.3 |
| `country area` | 1 | 4.3 |
| `agriculture area actually irrigated` | 1 | 4.3 |
| `inland waters` | 1 | 4.3 |
| `temporary meadows and pastures` | 1 | 4.3 |
| `agricultural land` | 1 | 4.3 |
| `cropland area certified organic` | 1 | 4.3 |
| `cropland` | 1 | 4.3 |
| `other land` | 1 | 4.3 |
| `agriculture` | 1 | 4.3 |
| `temporary crops` | 1 | 4.3 |
| `agriculture area certified organic` | 1 | 4.3 |
| `permanent crops` | 1 | 4.3 |
| `agriculture area under organic agric.` | 1 | 4.3 |
| `cropland area actually irrigated` | 1 | 4.3 |
| `land area equipped for irrigation` | 1 | 4.3 |
| `naturally regenerating forest` | 1 | 4.3 |
| `forest land` | 1 | 4.3 |
| `land area` | 1 | 4.3 |
| `arable land` | 1 | 4.3 |
| `planted forest` | 1 | 4.3 |
| `permanent meadows and pastures` | 1 | 4.3 |

### land_use_class (STRING, distinct=23)
| label_value | n | pct |
|---|---:|---:|
| `Cropland area under organic agric.` | 1 | 4.3 |
| `Temporary fallow` | 1 | 4.3 |
| `Country area` | 1 | 4.3 |
| `Agriculture area actually irrigated` | 1 | 4.3 |
| `Inland waters` | 1 | 4.3 |
| `Temporary meadows and pastures` | 1 | 4.3 |
| `Agricultural land` | 1 | 4.3 |
| `Cropland area certified organic` | 1 | 4.3 |
| `Cropland` | 1 | 4.3 |
| `Other land` | 1 | 4.3 |
| `Agriculture` | 1 | 4.3 |
| `Temporary crops` | 1 | 4.3 |
| `Agriculture area certified organic` | 1 | 4.3 |
| `Permanent crops` | 1 | 4.3 |
| `Agriculture area under organic agric.` | 1 | 4.3 |
| `Cropland area actually irrigated` | 1 | 4.3 |
| `Land area equipped for irrigation` | 1 | 4.3 |
| `Naturally regenerating forest` | 1 | 4.3 |
| `Forest land` | 1 | 4.3 |
| `Land area` | 1 | 4.3 |
| `Arable land` | 1 | 4.3 |
| `Planted Forest` | 1 | 4.3 |
| `Permanent meadows and pastures` | 1 | 4.3 |

### description (STRING, stats only, distinct=0)

## dim_livestock

### livestock_key (STRING, stats only, distinct=29, min=0e7ca3358e8cb5e835db2ff49904a173, max=fe480203b9d3f5f3088cb17ba2152029)

### species (STRING, distinct=29)
| label_value | n | pct |
|---|---:|---:|
| `1 2 5 6` | 1 | 3.4 |
| `1 3 6` | 1 | 3.4 |
| `1 2` | 1 | 3.4 |
| `2` | 1 | 3.4 |
| `1 2 3 5 6` | 1 | 3.4 |
| `1 3 5` | 1 | 3.4 |
| `1 5 6` | 1 | 3.4 |
| `1 2 3 5` | 1 | 3.4 |
| `cattle` | 1 | 3.4 |
| `1 6` | 1 | 3.4 |
| `1` | 1 | 3.4 |
| `2 3 6` | 1 | 3.4 |
| `1 2 5` | 1 | 3.4 |
| `1 2 3` | 1 | 3.4 |
| `1 3 5 6` | 1 | 3.4 |
| `1 2 3 5 6 7` | 1 | 3.4 |
| `1 2 3 6` | 1 | 3.4 |
| `pig` | 1 | 3.4 |
| `2 3 5 6` | 1 | 3.4 |
| `1 2 3 5 7` | 1 | 3.4 |
| `1 2 3 6 7` | 1 | 3.4 |
| `1 3` | 1 | 3.4 |
| `1 2 6` | 1 | 3.4 |
| `2 3 5` | 1 | 3.4 |
| `1 2 3 7` | 1 | 3.4 |
| `1 2 4 5 6 7` | 1 | 3.4 |
| `2 3` | 1 | 3.4 |
| `2 3 7` | 1 | 3.4 |
| `2 5 6` | 1 | 3.4 |

### loaded_at (TIMESTAMP, stats only, distinct=1, min=2026-08-24 14:23:00.239004+00, max=2026-08-24 14:23:00.239004+00)

## dim_sex

### sex_key (STRING, stats only, distinct=4, min=female, max=unknown)

### sex_label (STRING, distinct=4)
| label_value | n | pct |
|---|---:|---:|
| `Male` | 1 | 25.0 |
| `Female` | 1 | 25.0 |
| `Unknown` | 1 | 25.0 |
| `Total` | 1 | 25.0 |

### loaded_at (TIMESTAMP, stats only, distinct=1, min=2026-08-24 14:23:05.627158+00, max=2026-08-24 14:23:05.627158+00)

## dim_soil_property

### soil_property_key (STRING, stats only, distinct=70, min=03aa8a764c69b9947354c5f62f4bef10, max=fdc05140d9d1ff8ad21d7eccebef6e51)

### soil_property (STRING, distinct=22)
| label_value | n | pct |
|---|---:|---:|
| `cec` | 7 | 10.0 |
| `nitrogen` | 7 | 10.0 |
| `bdod` | 5 | 7.1 |
| `silt` | 5 | 7.1 |
| `clay` | 5 | 7.1 |
| `soc` | 5 | 7.1 |
| `phh2o` | 5 | 7.1 |
| `sand` | 5 | 7.1 |
| `sulfur` | 2 | 2.9 |
| `bulk_density` | 2 | 2.9 |
| `magnesium` | 2 | 2.9 |
| `stone_content` | 2 | 2.9 |
| `carbon_total` | 2 | 2.9 |
| `aluminum` | 2 | 2.9 |
| `ph` | 2 | 2.9 |
| `iron` | 2 | 2.9 |
| `potassium` | 2 | 2.9 |
| `zinc` | 2 | 2.9 |
| `carbon_organic` | 2 | 2.9 |
| `calcium` | 2 | 2.9 |
| `texture_class` | 1 | 1.4 |
| `bedrock_depth` | 1 | 1.4 |

### depth (STRING, distinct=7)
| label_value | n | pct |
|---|---:|---:|
| `0-20cm` | 16 | 22.9 |
| `20-50cm` | 14 | 20.0 |
| `0_5cm` | 8 | 11.4 |
| `15_30cm` | 8 | 11.4 |
| `30_60cm` | 8 | 11.4 |
| `5_15cm` | 8 | 11.4 |
| `60_100cm` | 8 | 11.4 |

### loaded_at (TIMESTAMP, stats only, distinct=1, min=2026-08-24 21:57:15.796122+00, max=2026-08-24 21:57:15.796122+00)
