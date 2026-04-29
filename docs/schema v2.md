
# LandWeaver Schema V2
Simplify the YAML configuration and improve structure for  editor.

## 1. Modifiers
**Goal:** Eliminate Modifier indirection. Coupling the effect with the profile allows for 
simpler resource resolution.

*   **V1:** Modifiers are nested dictionaries inside surfaces; they reference a separate `modifier_profiles` 
section.
*   **V2:** Surfaces contain a simple list of strings. These strings reference a top-level `modifiers` 
section where the `op` (effect) is defined.

**V1 (Old):**
```yaml
surfaces:
  humid:
    modifiers:
      - effect: color_mottle
        mod_profile: forest_mod

modifier_profiles:
  forest_mod:
    intensity: 20.0
    shift_vector: [1.0, 0.8, 0.5]
    noise_id: biome
```

**V2 (New):**
```yaml
surfaces:
  humid:
    modifiers: ["forest_mod"]  # Simplified reference list

modifiers:  # Renamed from modifier_profiles
  forest_mod:
    op: color_mottle  # Effect is now part of the profile
    intensity: 20.0
    shift_vector: [1.0, 0.8, 0.5]
    noise_id: biome
```

---

## 2. Thematic Factors
**Goal:** Explicitly identify theme categories. This distinguishes "objects" from "tuning parameters,"
enabling the Factor Engine to perform "Competitive Smoothing" (Max-Alpha resolution) more efficiently.

*   **V1:** Categories are hidden inside a generic `params` block.
*   **V2:** Categories are placed in a dedicated `categories` block.

**V1 (Old):**
```yaml
factors:
  theme_composite:
    factor_builder: theme_composite
    sources: [theme]
    params:
      water:
        enabled: true
        blur_px: 3.0
```

**V2 (New):**
```yaml
factors:
  theme_composite:
    op: theme_composite  # Standardized
    sources: [theme]
    categories:  # Explicitly named group
      water:
        enabled: true
        blur_px: 3.0
```

---

## 3.  Standardization of Operation name
**Goal:** Create a single, universal "operation" selector for all functional blocks. This allows the UI 
Orchestrator and the RenderServer to use the same logic for every section of the file.

*   **V1:** Operation keys are fragmented: `factor_builder`, `surface_builder`, `effect`, `blend_op`.
*   **V2:** All functional discriminators are renamed to **`op`**.

| V1                           | V2   |
|:-----------------------------|:-----|
| `blend_op` (Pipeline)        | `op` |
| `factor_builder` (Factors)   | `op` |
| `surface_builder` (Surfaces) | `op` |
| `effect` (Modifiers)         | `op` |

---

## 4.  Blend Operation Name simplification
**Goal:** Create user friendly blend operation names

*   **V1:** Blend Operation names are technical: `lerp`, etc.
*   **V2:** Blend Operation names are easier for Analysts and Designers
Note: the edit panel will replace "_" with space and use Title Case for displays

| V1 Name                   | V2 Name               | Display               |
|---------------------------|-----------------------|-----------------------|
| `lerp_surfaces`           | `blend_surfaces`      | `Blend Surfaces`      |
| `lerp`                    | `blend_overlay`       | `Blend Overlay`       |
| `lerp_buffers`            | `blend_buffers`       | `Blend Buffers`       |
| `add_specular_highlights` | `specular_highlights` | `Specular Highlights` |
| `apply_zonal_gradient`    | `gradient_fill`       | `Gradient Fill`       |
| No Change:                |                       |                       |
| `create_buffer`           | `create_buffer`       | `Create Buffer`       |
| `multiply`                | `multiply`            | `Multiply`            |
| `alpha_over`              | `alpha_over`          | `Alpha Over`          |


---
### Final "V2 Schema" Architecture (The Result)
By following these  steps, the `render.yml` transforms from a complex, nested data structure into 
a flat **Node Graph**. Every object follows a predictable pattern:
1.  **Identity:** (`name`, `desc`)
2.  **Type:** (`op`)
3.  **Infrastructure/Links:** (`sources`, `factor`, `modifiers`)
4.  **Tuning:** (Flattened parameters or specific `categories`)

This structure allows the **Editor** to be 90% generic code, making it stable and easy
to extend.