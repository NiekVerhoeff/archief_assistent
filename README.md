# Archiefassistent

**Archiefassistent** is a schema-driven metadata extraction platform for archivists.  
It uses Large Language Models (LLMs) to automatically extract, validate, and structure archival metadata from digital files — fully configurable and export-ready.

The system is designed to support archival workflows where flexibility, transparency, and standards compliance are essential.

---

## What It Does

Archiefassistent allows archivists to:

- 📂 Upload and preprocess digital files  
- 🧠 Extract metadata using LLMs  
- 📑 Define custom JSON Schemas for extraction  
- 🏗 Extract nested and structured metadata (e.g., multiple addresses, persons, classifications)  
- 🔎 Use embeddings to retrieve relevant text fragments  
- ✏️ Review, validate, and edit extracted metadata  
- ⚠️ Flag low-confidence or invalid values  
- 🔄 Map extracted metadata to your desired export formats  
- 📤 Export structured records (e.g., JSON aligned with archival models)  

---

## Why This Project Exists

Archival metadata extraction is complex:

- Documents are long and noisy  
- Metadata structures differ per institution  
- Standards evolve  
- Manual cataloguing is time-consuming  

Archiefassistent addresses this by combining:

- LLM-based extraction  
- Schema-driven control  
- Chunking and embedding-based retrieval  
- Validation and aggregation logic  
- Mapping to desired export profiles 

The result is a system that keeps humans in control while leveraging AI for efficiency.

---

## Core Architecture

The system follows a modular pipeline:

### 1. Preprocessing
- Files are uploaded  
- Text is extracted  
- Large chunks created (for summarization)  
- Small chunks created (for embedding & targeted extraction)  
- Embeddings stored in database  
- Technical metadata extracted  

### 2. Extraction
- LLM extracts metadata according to a user-defined JSON Schema  
- Per-chunk extraction  
- Schema-aware aggregation  
- Optional confidence tracking  

### 3. Validation & Editing
- Results shown in schema-driven table  
- Cell-level validation (dates, enums, language codes, etc.)  
- Low-confidence highlighting  
- Manual correction possible  

### 4. Export
- Profiles define export structure  
- User maps extracted fields to targets  
- Structured export generated  

---

## Key Design Principles

- **Schema-first**: Extraction adapts to user-defined JSON Schemas.  
- **Archivist control**: Human review is central.  
- **Nested metadata support**: Addresses, persons, classifications.  
- **Chunk-aware aggregation**: Intelligent merging across document parts.  
- **Embeddings for targeted retrieval**: Improve precision for dates, entities, identifiers.  
- **Standards alignment**: Define export profiles.  
- **Modular architecture**: Worker-based job processing.  

---

## Technology Stack

- Python  
- Streamlit (UI)  
- SQLite (job + record storage)  
- Ollama (local LLM inference)  
- JSON Schema   
- Embedding-based retrieval  
- Modular job/worker architecture  

---

## Current Capabilities

- Custom schema generation via LLM  
- Nested object extraction  
- Multiple chunk strategies  
- Embedding storage and retrieval  
- Confidence-based validation  
- Schema-driven results table  
- Export mapping  

---

## Project Status

Active development.  
Designed for experimentation and practical archival workflows.
