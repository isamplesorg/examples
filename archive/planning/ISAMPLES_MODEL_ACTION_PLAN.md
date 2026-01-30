# iSamples Model Correction Action Plan

## Overview
This action plan addresses the correction needed after discovering that the iSamples metadata model is **domain-agnostic**, not archaeology-specific as previously documented in our notebooks and code.

## Key Understanding Update

### Previous (Incorrect) Understanding
- Treated `MaterialSampleRecord`, `SamplingEvent`, `GeospatialCoordLocation`, `SamplingSite` as archaeology-specific entity types
- Documented predicates like `produced_by`, `sample_location` as OpenContext-specific
- Suggested the model was customized for archaeological data

### Corrected Understanding
1. **PQG Framework**: Generic property graph representation (s/p/o/n)
2. **iSamples Model**: Domain-agnostic metadata standard for ALL scientific material samples
3. **Domain Data**: OpenContext, SESAR, GEOME populate the model with domain-specific VALUES

## Phase 1: Documentation Updates (Immediate)

### Notebooks to Update
- [ ] `examples/basic/oc_parquet_analysis_enhanced.ipynb`
  - Fix section "Key Distinction: Generic PQG vs OpenContext-Specific"
  - Update all comments suggesting entity types are archaeology-specific
  - Clarify that OpenContext uses standard iSamples model with archaeological data

- [ ] `examples/basic/oc_parquet_analysis.ipynb`
  - Update introductory documentation
  - Fix inline comments about entity types

- [ ] `examples/basic/geoparquet.ipynb`
  - Verify no misleading archaeology-specific claims
  - Add note about cross-domain capabilities

### Documentation Files
- [x] `README.md` - Updated to reflect domain-agnostic nature
- [ ] `examples/README.md` - Update notebook descriptions
- [ ] `STATUS.md` - Add note about model understanding correction
- [ ] `CROSS_REPO_ALIGNMENT.md` - Ensure consistency with new understanding

## Phase 2: Code Comment Updates

### Priority Files
- [ ] All notebooks with SQL/Ibis queries
  - Change comments from "OpenContext-specific entity" to "iSamples entity"
  - Update "OpenContext predicate" to "iSamples predicate"
  - Fix variable names like `archaeological_sites` → `sampling_sites`

### Example Changes Needed
```python
# OLD (incorrect):
samples = oc_pqg.filter(_.otype == 'MaterialSampleRecord')  # OpenContext entity

# NEW (correct):
samples = oc_pqg.filter(_.otype == 'MaterialSampleRecord')  # iSamples entity (archaeological data)
```

## Phase 3: Enhanced Cross-Domain Examples

### New Notebooks/Sections to Create
- [ ] Add section showing how same queries work across domains
- [ ] Create comparison: archaeological vs geological samples using same model
- [ ] Document which fields are domain-universal vs domain-specific values

### Query Pattern Documentation
- [ ] Document universal graph traversal patterns
- [ ] Show how predicates work across domains
- [ ] Create reference table: Entity Type → Example Values by Domain

## Phase 4: Testing & Validation

### Verification Tasks
- [ ] Test existing queries still work correctly
- [ ] Verify no functional breaks from documentation changes
- [ ] If available, test with non-archaeological iSamples data
- [ ] Validate cross-domain query capabilities

### Performance Testing
- [ ] Ensure query performance unchanged
- [ ] Document any optimization opportunities from new understanding

## Phase 5: Communication & Education

### Internal Documentation
- [x] Dev journal entry (2025-09-26) documenting discovery
- [x] Project journal update with correction
- [ ] Add "Model Clarification" section to main docs

### External Communication
- [ ] Consider blog post or documentation update for iSamples community
- [ ] Update any presentations or tutorials
- [ ] Notify collaborators of corrected understanding

## Implementation Priority

1. **Immediate** (Today):
   - [x] Document discovery in dev journal
   - [x] Update main README
   - [x] Create this action plan

2. **High Priority** (This Week):
   - [ ] Fix oc_parquet_analysis_enhanced.ipynb documentation
   - [ ] Update all notebook comments
   - [ ] Test for any functional impacts

3. **Medium Priority** (Next 2 Weeks):
   - [ ] Create cross-domain examples
   - [ ] Enhance documentation with domain comparisons
   - [ ] Update all secondary documentation

4. **Low Priority** (As Time Permits):
   - [ ] Refactor variable names for clarity
   - [ ] Create educational materials
   - [ ] Consider broader community communication

## Success Criteria

- All documentation accurately reflects domain-agnostic nature of iSamples
- No misleading references to "archaeology-specific" entity types
- Clear explanation of the three-layer architecture (PQG → iSamples → Domain Data)
- Examples demonstrate cross-domain capabilities
- Community understanding aligned with correct model

## Notes

- This correction actually makes the iSamples model MORE powerful - it's a universal framework
- Emphasize the positive: enables cross-domain discovery and integration
- Use this as an opportunity to showcase the model's flexibility

## References

- iSamples LinkML Schema: https://isamplesorg.github.io/metadata/
- PQG Documentation: `/Users/raymondyee/C/src/iSamples/pqg/isamples/README.md`
- Eric Kansa discussion: 2025-09-26 (clarified domain-agnostic nature)