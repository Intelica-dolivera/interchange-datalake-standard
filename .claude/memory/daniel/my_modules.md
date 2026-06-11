---
name: daniel-modules
description: Módulos desarrollados por Daniel Olivera — código, estado y decisiones propias
metadata:
  type: project
---

# Módulos de Daniel

## data_quality (en desarrollo)

**Estado:** planificado — inicio previsto próximas sesiones
**Descripción:** módulo de calidad de datos sobre el pipeline de interchange
**Rama/path esperado:** por definir

**Why:** validar que los datos procesados por el pipeline (clean → calculate → interchange) cumplen
reglas de negocio antes de llegar a operational, evitando que datos incorrectos lleguen a Athena.

**How to apply:** cuando se trabaje en data_quality, este archivo es la referencia del estado y
decisiones tomadas. Las decisiones arquitectónicas propias del módulo se documentan aquí (no en
decisions.md de Julio).

---

_Agregar nuevas secciones cuando se inicien nuevos módulos._
