# Changelog

- 2.2.1 [STABILITY] Implemented simplified Schema V2.  See schema v2.md for details.
- 2.2 [STABILITY] Cleaned up separation of Pipeline and RenderSystem. Added Idle shutdown.
- 2.1 [PERF] Improved Noise creation speed
- 2.0 [STABILITY] Combined Factor and Logic config sections.
- 1.9 [STABILITY] Added detailed render config verification
- 1.8 [FEAT] Added ability to add/delete pipeline steps without restarting pipeline
- 1.7 [STABILITY] Refactored project into a Pipeline package, Render Package and a Common package.
- 1.6 [STABILITY] Better SHM partitioning and  memory scaling. Fixed parameter passing.
- 1.5 [FEAT] Added High-fidelity theme smoothing and noise injection logic
- 1.4 [STABILITY] Hardened error handling and socket recovery
- 1.3 [PERF] Transitioned to Client/Daemon architecture to allow persisted state and tile caches
- 1.2 [STABILITY] Internal refactoring and resource cleanup
- 1.1 [PERF] Implemented multiprocessing worker pools
- 1.0 [FEAT] Initial functional release