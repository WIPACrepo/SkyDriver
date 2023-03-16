# Changelog

<!--next-version-placeholder-->

## v0.0.38 (2023-03-16)
### Other
* Clientmanager: fix sending cluster info, rollback dumping logs ([`98b5f65`](https://github.com/WIPACrepo/SkyDriver/commit/98b5f659af8b10c677954e817ecd9cb31c4b0368))

## v0.0.37 (2023-03-15)
### Other
* Clientmanager: use `/bin/bash` ([`4d8e25c`](https://github.com/WIPACrepo/SkyDriver/commit/4d8e25c95e1de53eb4da1f97f3f38b0c5f90d2b1))

## v0.0.36 (2023-03-15)
### Other
* Clientmanager: using logging for condor logs ([`b7dac93`](https://github.com/WIPACrepo/SkyDriver/commit/b7dac938b8baec350c9ad40323a6645ab04084d5))

## v0.0.35 (2023-03-15)
### Other
* <bot> update requirements.txt ([`a465df5`](https://github.com/WIPACrepo/SkyDriver/commit/a465df5089e74b80aac8bb3ce300be2f8f9c753e))
* <bot> update requirements-tests.txt ([`e54b93a`](https://github.com/WIPACrepo/SkyDriver/commit/e54b93a0d2311d06a4398b5a8e21c7402f21236c))
* <bot> update requirements-clientmanager.txt ([`0e910d7`](https://github.com/WIPACrepo/SkyDriver/commit/0e910d749627737fa199665e4d61338a30231f4f))
* It's okay to run `py-setup` on `main` ([`8d22946`](https://github.com/WIPACrepo/SkyDriver/commit/8d229467dcf0d7b9246659bfbe021bf11147886f))
* Pin `typeguard==2.13.3` until #15 ([`b67b6bd`](https://github.com/WIPACrepo/SkyDriver/commit/b67b6bd9582ae870c990bafba3ed48da5cd2e496))
* Clientmanager: dump condor files when done launching ([`4963a7e`](https://github.com/WIPACrepo/SkyDriver/commit/4963a7ec53bb389be2936ba6aeb7908ad24f973e))

## v0.0.34 (2023-03-10)
### Other
* Cast to list ([`923189c`](https://github.com/WIPACrepo/SkyDriver/commit/923189c2ff8ead25574a9b099ef9a25dc627304f))

## v0.0.33 (2023-03-10)
### Other
* Add `job_description.jobs()` ([`542e1fc`](https://github.com/WIPACrepo/SkyDriver/commit/542e1fccc734ae99b662826da37395a0c51b5e15))

## v0.0.32 (2023-03-10)
### Other
* Add `spool()` call ([`1a61cfd`](https://github.com/WIPACrepo/SkyDriver/commit/1a61cfda2e41cd5c5d75762ad92dabecb4447de3))
* Use `spool=True` ([`20265cf`](https://github.com/WIPACrepo/SkyDriver/commit/20265cfd93e182e4631ec0a60deb13431bc8c7b4))

## v0.0.31 (2023-03-10)
### Other
* Add quotes to condor submit ([`bc0b538`](https://github.com/WIPACrepo/SkyDriver/commit/bc0b538356ac104a89bbb79446e848892f5881b9))

## v0.0.30 (2023-03-10)
### Other
* Create `~/.condor/tokens.d/` if needed ([`de17c11`](https://github.com/WIPACrepo/SkyDriver/commit/de17c1196e15e49f64996f9f8f82d0737903c0d6))

## v0.0.29 (2023-03-10)
### Other
* Update tests ([`84fdef4`](https://github.com/WIPACrepo/SkyDriver/commit/84fdef423c3b5d89c0537d3c1dd6cbf9276b8ca3))
* Forward `condor_token`/`CONDOR_TOKEN` to client starter ([`ebd6e47`](https://github.com/WIPACrepo/SkyDriver/commit/ebd6e474a5b1057c25505439abe474b9a9aa9d4b))

## v0.0.28 (2023-03-10)
### Other
* Fix test; mypy ([`17d358b`](https://github.com/WIPACrepo/SkyDriver/commit/17d358b888bcff7ebca4023e7d4b91eabbdd8b7b))
* If requestor attempts empty result, just ignore it ([`e8c8d2d`](https://github.com/WIPACrepo/SkyDriver/commit/e8c8d2d263012001aaa25ca5cf517e0091a2552c))

## v0.0.27 (2023-03-10)
### Other
* Clientmanager: fix scanid retrieval ([`0891275`](https://github.com/WIPACrepo/SkyDriver/commit/08912759330a83f46039fc08e938839de370e68d))
* Fix tests ([`4811bf9`](https://github.com/WIPACrepo/SkyDriver/commit/4811bf976f0c0ce56ad58615f26a9f5038481576))
* Re-key as `skyscan_result` ([`3e6b360`](https://github.com/WIPACrepo/SkyDriver/commit/3e6b360848ecbf259447025550874b7c0ec60c0d))

## v0.0.26 (2023-03-09)
### Other
* Fix tests ([`cbca980`](https://github.com/WIPACrepo/SkyDriver/commit/cbca980b97ff2813ac5d991d2f7cd52e75cbf0c5))
* Point to `ghcr.io/wipacrepo/skydriver:latest` for clientmanager ([`9580f83`](https://github.com/WIPACrepo/SkyDriver/commit/9580f836681ea59a94a6e80b314a666201842608))
* ClientManager / `client_starter.py` ([#14](https://github.com/WIPACrepo/SkyDriver/issues/14)) ([`bbb2cb1`](https://github.com/WIPACrepo/SkyDriver/commit/bbb2cb13a1de699bb8ac340ffd353d2bf5585c9e))

## v0.0.25 (2023-03-09)
### Other
* Add `spec.template.metadata.labels.app = scanner-instance` ([`8b84f1e`](https://github.com/WIPACrepo/SkyDriver/commit/8b84f1e9eadf46d985e2dd6e00e7fb2380105232))

## v0.0.24 (2023-03-09)
### Other
* Make `processing_stats.rate` optional ([`f7b3c24`](https://github.com/WIPACrepo/SkyDriver/commit/f7b3c244b9b2e1be577c348c424525b5f81bfc69))

## v0.0.23 (2023-03-09)
### Other
* Allow scanner account to GET @ `/scan/manifest/<scan-id>` ([`17d0bd3`](https://github.com/WIPACrepo/SkyDriver/commit/17d0bd37fd8c3008b18938fd9596ad5169ec8eca))

## v0.0.22 (2023-03-09)
### Other
* Fix tests ([`98d60fc`](https://github.com/WIPACrepo/SkyDriver/commit/98d60fc8fff31a45c30eed38c13887109eefe4f7))
* Fix volume mount (`/common-space`) ([`e3306cb`](https://github.com/WIPACrepo/SkyDriver/commit/e3306cb03b7549db8aa7d1d16ec1e7ae23348131))

## v0.0.21 (2023-03-09)
### Other
* Fix tests ([`08074f6`](https://github.com/WIPACrepo/SkyDriver/commit/08074f698a27a18c53a28c19b02189905fd78a36))
* Use one directory for the shared volume ([`a54a20c`](https://github.com/WIPACrepo/SkyDriver/commit/a54a20cad3f806cf4f05f7281292d581bb4d93df))

## v0.0.20 (2023-03-08)
### Other
* Set backoff limit to 1 to stop pod restarts ([`f18d771`](https://github.com/WIPACrepo/SkyDriver/commit/f18d7716ab804ae24c0e861dfb9d170c7da78ccd))
* Name each job "skyscan-{scan_id}" ([`28f93fe`](https://github.com/WIPACrepo/SkyDriver/commit/28f93feb17e6e225f8235ddd74422d38022b700e))

## v0.0.19 (2023-03-08)
### Other
* Don't let ArgoCD prune dynamic jobs ([`8989bd1`](https://github.com/WIPACrepo/SkyDriver/commit/8989bd1f4506963654a076f55210729f2cb25b22))

## v0.0.18 (2023-03-08)
### Other
* Add labels for k8s resource tracking ([`8596c7a`](https://github.com/WIPACrepo/SkyDriver/commit/8596c7a58a2a4b3cc37d67bd931ddc1a89a9eefe))

## v0.0.17 (2023-03-08)
### Other
* Make k8s job containers' names unique ([`4b86551`](https://github.com/WIPACrepo/SkyDriver/commit/4b865512db62aa5c411e0a044b7ea16ce89d0f7d))

## v0.0.16 (2023-03-08)
### Other
* Send 500 error if scanner k8s job fails to launch ([`4ab57ad`](https://github.com/WIPACrepo/SkyDriver/commit/4ab57ad4249860a1b2ee733abea75c0c3df747b2))
* Make index `event_run_index` non-unique ([`354da27`](https://github.com/WIPACrepo/SkyDriver/commit/354da2773665cc3acc9fc8ec46620f46f63becde))

## v0.0.15 (2023-03-08)
### Other
* Update Requestor Auth ([`6c208b2`](https://github.com/WIPACrepo/SkyDriver/commit/6c208b2d1102e628149d23baa3418123c3ed470e))
* Merge remote-tracking branch 'origin/main' ([`51f911b`](https://github.com/WIPACrepo/SkyDriver/commit/51f911b37f7ba09ccd5cbe19bf2374dfe3ed1c78))
* Appease mypy ([`929d164`](https://github.com/WIPACrepo/SkyDriver/commit/929d164769114224c2d4ee0523846a0127944919))

## v0.0.14 (2023-03-07)
### Other
* Update type hinting ([`9b4f318`](https://github.com/WIPACrepo/SkyDriver/commit/9b4f3188517b7f81ec69979ca585271a4ab9f5fe))
* Remove manual env var logging ([`89e9270`](https://github.com/WIPACrepo/SkyDriver/commit/89e927013f67fc675dae9dad02e6320f9ec256c9))
* CI `concurrency`: don't cancel on main/master/default ([`438a5d5`](https://github.com/WIPACrepo/SkyDriver/commit/438a5d557e15c03f7fd51f0fd67b1e2d5c202680))

## v0.0.13 (2023-03-07)
### Other
* Kube API Non-Default Configuration ([`26b7d96`](https://github.com/WIPACrepo/SkyDriver/commit/26b7d9605084bf5059b005dd79d97c3f33fb6c92))

## v0.0.12 (2023-03-07)
### Other
* Merge remote-tracking branch 'origin/main' ([`001aed3`](https://github.com/WIPACrepo/SkyDriver/commit/001aed3ce553ada10ae9adb86a642c4a75ec9434))
* Add More Logging ([`aad73c5`](https://github.com/WIPACrepo/SkyDriver/commit/aad73c53dc809e86487979aa82aa8c859b9c7020))

## v0.0.11 (2023-03-07)
### Other
* Merge remote-tracking branch 'origin/main' ([`0ca8014`](https://github.com/WIPACrepo/SkyDriver/commit/0ca8014b8cbfcd0238081d101d1979b3f0970e25))
* Kube API Default Configuration ([`5230e62`](https://github.com/WIPACrepo/SkyDriver/commit/5230e624e2634f164faa6e6b7d12ff40cce9d307))
* Mypy ([`4a2703d`](https://github.com/WIPACrepo/SkyDriver/commit/4a2703d8c09d6db0a6ea732f0edc0c8a5b3b722e))

## v0.0.10 (2023-03-07)
### Other
* Kube API Quick Fix ([`8fbb662`](https://github.com/WIPACrepo/SkyDriver/commit/8fbb662c8996eb9b1e3cfcfc647aeb193636dbac))

## v0.0.9 (2023-03-07)
### Other
* Env Var Quick Fix ([`daa2e1b`](https://github.com/WIPACrepo/SkyDriver/commit/daa2e1ba45b0b7ac3d2af74501b30993cf1f1f98))

## v0.0.8 (2023-03-06)
### Other
* Use `PERSONAL_ACCESS_TOKEN` for bot pt-2 ([`1c5fc65`](https://github.com/WIPACrepo/SkyDriver/commit/1c5fc65c4598613defe263b74c1d5942f1950cd7))

## v0.0.7 (2023-03-06)
### Other
* Use `PERSONAL_ACCESS_TOKEN` for bot ([`26822fa`](https://github.com/WIPACrepo/SkyDriver/commit/26822fa4ee60a706bbfb26e887e1c92c00a06e76))

## v0.0.6 (2023-03-06)
### Other
* Add Dockerfile & Publishing ([#11](https://github.com/WIPACrepo/SkyDriver/issues/11)) ([`eb6a122`](https://github.com/WIPACrepo/SkyDriver/commit/eb6a122a6ba24beae28b080c9a6784b379feb079))

## v0.0.5 (2023-03-01)
### Other
* Auth Part 2 ([#9](https://github.com/WIPACrepo/SkyDriver/issues/9)) ([`d8cde67`](https://github.com/WIPACrepo/SkyDriver/commit/d8cde67f295209651b10bc62fb00e9de7dc880e0))

## v0.0.4 (2023-02-15)
### Other
* Implement Auth ([#7](https://github.com/WIPACrepo/SkyDriver/issues/7)) ([`d92db06`](https://github.com/WIPACrepo/SkyDriver/commit/d92db0663f75c801528610a60ab8542ac52543d9))

## v0.0.3 (2023-02-06)


## v0.0.2 (2023-01-27)


## v0.0.1 (2022-12-14)

