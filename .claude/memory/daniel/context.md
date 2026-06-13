---
name: daniel-context
description: Quién es Daniel Olivera, su rol en el proyecto y cómo trabaja con este repo
metadata:
  type: user
---

# Contexto de Daniel Olivera

## Identidad y rol

- **Nombre:** Daniel Olivera
- **Organización:** Intelica IT
- **Email:** dolivera95@gmail.com
- **Rol en este proyecto:** desarrollador independiente trabajando en módulos propios sobre la base del pipeline de interchange

## Estructura de repos

- **Su repo (origin):** `https://github.com/Intelica-dolivera/interchange-datalake-standard.git`
- **Repo base (upstream):** `https://github.com/intelica-jcardenas/interchange-datalake-aws` (Julio Cardenas)
- **Relación:** fork de trabajo — Daniel avanza sus propios módulos sin modificar el repo de Julio

## Estrategia de memoria

- Los archivos de Julio (`CLAUDE.md`, `.claude/memory/decisions.md`, `.claude/memory/gotchas.md`, `.claude/memory/manual_execution.md`) son **upstream-owned**: Daniel los lee pero no los modifica.
- Los archivos de Daniel viven en `.claude/memory/daniel/` — Julio nunca los toca.
- Notas temporales y locales van en `.claude/local/` (gitignored).

## Flujo de sincronización

- **Sync con AWS** (`sync-lambdas.ps1` / `sync-glue.ps1`): obtener código desplegado por cualquier desarrollador
- **Sync con Julio** (`git pull upstream main`): obtener código + memorias de Julio sobre módulos nuevos
- Ambos flujos coexisten y no generan conflictos por la separación de archivos

## Cómo colaborar con Daniel

- Explicar en términos del pipeline (etapas, S3 paths, DynamoDB tables) — ya tiene contexto sólido del sistema
- Prefiere propuestas antes de ejecutar cambios
- Trabaja en WSL2 (Ubuntu sobre Windows) — usar **bash/AWS CLI**, nunca PowerShell ni scripts .ps1
- AWS profile de Daniel: `interchange-dev` (NO `itx-dev` que es el de Julio)
- Runtime AWS: región `eu-south-2`, cuenta de prueba
