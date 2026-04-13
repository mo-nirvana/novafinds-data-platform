# CI/CD Integration Guide

## Overview

This guide shows how to automate deployment of the NovaFinds pipeline using popular CI/CD platforms.

**Workflow**:
1. **On PR**: Deploy to dev, run unit tests
2. **On merge to main**: Deploy to staging, run full integration tests
3. **On tag/release**: Deploy to production

---

## GitHub Actions

### Setup Secrets

Go to **Settings → Secrets and variables → Actions** and add:

* `DATABRICKS_HOST`: Your Databricks workspace URL (e.g., `https://adb-123456789.azuredatabricks.net`)
* `DATABRICKS_TOKEN`: Personal access token or service principal token

### Workflow File: `.github/workflows/deploy.yml`

```yaml
name: NovaFinds Pipeline CI/CD

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]
  release:
    types: [published]

jobs:
  validate:
    name: Validate Bundle
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
          echo "$HOME/.databricks/bin" >> $GITHUB_PATH
      
      - name: Validate Dev Bundle
        run: databricks bundle validate -t dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

  deploy-dev:
    name: Deploy to Dev
    runs-on: ubuntu-latest
    needs: validate
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
          echo "$HOME/.databricks/bin" >> $GITHUB_PATH
      
      - name: Deploy to Dev
        run: databricks bundle deploy -t dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      
      - name: Run Unit Tests
        run: databricks bundle run novafinds_unit_tests -t dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

  deploy-stage:
    name: Deploy to Staging
    runs-on: ubuntu-latest
    needs: validate
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
          echo "$HOME/.databricks/bin" >> $GITHUB_PATH
      
      - name: Deploy to Staging
        run: databricks bundle deploy -t stage
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      
      - name: Run Integration Tests
        run: databricks bundle run novafinds_integration_pipeline -t stage
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

  deploy-prod:
    name: Deploy to Production
    runs-on: ubuntu-latest
    needs: validate
    if: github.event_name == 'release' && github.event.action == 'published'
    environment:
      name: production
      url: https://adb-123456789.azuredatabricks.net
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
          echo "$HOME/.databricks/bin" >> $GITHUB_PATH
      
      - name: Deploy to Production
        run: databricks bundle deploy -t prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
      
      - name: Create Deployment Tag
        run: |
          echo "Deployed to production at $(date)"
          echo "Release: ${{ github.event.release.tag_name }}"
```

---

## Azure DevOps

### Setup Variables

Go to **Pipelines → Library** and create variable group `databricks-credentials`:

* `DATABRICKS_HOST`: Your Databricks workspace URL
* `DATABRICKS_TOKEN`: Personal access token (mark as secret)

### Pipeline File: `azure-pipelines.yml`

```yaml
trigger:
  branches:
    include:
      - main
  tags:
    include:
      - v*

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: databricks-credentials

stages:
  - stage: Validate
    jobs:
      - job: ValidateBundle
        steps:
          - task: Bash@3
            displayName: 'Install Databricks CLI'
            inputs:
              targetType: 'inline'
              script: |
                curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                echo "##vso[task.prependpath]$HOME/.databricks/bin"
          
          - task: Bash@3
            displayName: 'Validate Bundle'
            inputs:
              targetType: 'inline'
              script: 'databricks bundle validate -t dev'
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)

  - stage: DeployStaging
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployToStage
        environment: 'staging'
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                
                - task: Bash@3
                  displayName: 'Install Databricks CLI'
                  inputs:
                    targetType: 'inline'
                    script: |
                      curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                      echo "##vso[task.prependpath]$HOME/.databricks/bin"
                
                - task: Bash@3
                  displayName: 'Deploy to Staging'
                  inputs:
                    targetType: 'inline'
                    script: 'databricks bundle deploy -t stage'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
                
                - task: Bash@3
                  displayName: 'Run Integration Tests'
                  inputs:
                    targetType: 'inline'
                    script: 'databricks bundle run novafinds_integration_pipeline -t stage'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)

  - stage: DeployProduction
    dependsOn: Validate
    condition: and(succeeded(), startsWith(variables['Build.SourceBranch'], 'refs/tags/v'))
    jobs:
      - deployment: DeployToProd
        environment: 'production'
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                
                - task: Bash@3
                  displayName: 'Install Databricks CLI'
                  inputs:
                    targetType: 'inline'
                    script: |
                      curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                      echo "##vso[task.prependpath]$HOME/.databricks/bin"
                
                - task: Bash@3
                  displayName: 'Deploy to Production'
                  inputs:
                    targetType: 'inline'
                    script: 'databricks bundle deploy -t prod'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
```

---

## GitLab CI

### Setup Variables

Go to **Settings → CI/CD → Variables** and add:

* `DATABRICKS_HOST`: Your Databricks workspace URL
* `DATABRICKS_TOKEN`: Personal access token (mark as masked and protected)

### Pipeline File: `.gitlab-ci.yml`

```yaml
image: ubuntu:latest

stages:
  - validate
  - deploy-dev
  - deploy-stage
  - deploy-prod

before_script:
  - apt-get update && apt-get install -y curl
  - curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
  - export PATH="$HOME/.databricks/bin:$PATH"

validate:
  stage: validate
  script:
    - databricks bundle validate -t dev
  only:
    - merge_requests
    - main
    - tags

deploy:dev:
  stage: deploy-dev
  script:
    - databricks bundle deploy -t dev
    - databricks bundle run novafinds_unit_tests -t dev
  only:
    - merge_requests
  environment:
    name: development

deploy:stage:
  stage: deploy-stage
  script:
    - databricks bundle deploy -t stage
    - databricks bundle run novafinds_integration_pipeline -t stage
  only:
    - main
  environment:
    name: staging

deploy:prod:
  stage: deploy-prod
  script:
    - databricks bundle deploy -t prod
  only:
    - tags
  environment:
    name: production
  when: manual
```

---

## Best Practices

### 1. Use Service Principals

Instead of personal access tokens, use service principals for production deployments:

```bash
# Azure AD service principal
export DATABRICKS_HOST="https://adb-123456789.azuredatabricks.net"
export ARM_CLIENT_ID="<application-id>"
export ARM_CLIENT_SECRET="<client-secret>"
export ARM_TENANT_ID="<tenant-id>"

databricks bundle deploy -t prod
```

### 2. Separate Secrets per Environment

Create different secrets for dev/stage/prod:

* `DATABRICKS_DEV_TOKEN`
* `DATABRICKS_STAGE_TOKEN`
* `DATABRICKS_PROD_TOKEN`

### 3. Add Approval Gates

For production deployments, require manual approval:

* **GitHub**: Use environments with required reviewers
* **Azure DevOps**: Add approval checks to production environment
* **GitLab**: Use `when: manual` for production stage

### 4. Tag Releases

Use semantic versioning for production releases:

```bash
git tag -a v1.0.0 -m "Release 1.0.0"
git push origin v1.0.0
```

### 5. Monitor Pipeline Runs

Add monitoring to your CI/CD pipeline:

```yaml
- name: Check Pipeline Status
  run: |
    RUN_ID=$(databricks runs list --limit 1 --output json | jq -r '.runs[0].run_id')
    databricks runs get --run-id $RUN_ID
```

---

## Troubleshooting

### Authentication Fails

* Verify `DATABRICKS_HOST` includes `https://` and no trailing slash
* Check token has not expired (tokens expire after 90 days by default)
* Ensure service principal has workspace access

### Bundle Deploy Fails

* Check YAML syntax: `databricks bundle validate -t dev`
* Verify all referenced files exist in the repository
* Ensure target catalog exists in the workspace

### Pipeline Run Fails

* Check Databricks workspace event logs
* Verify data sources are accessible
* Check compute cluster permissions

---

## Additional Resources

* [Databricks CI/CD Best Practices](https://docs.databricks.com/dev-tools/bundles/ci-cd.html)
* [GitHub Actions for Databricks](https://github.com/databricks/setup-cli)
* [Azure DevOps Databricks Extension](https://marketplace.visualstudio.com/items?itemName=riserrad.azdo-databricks)