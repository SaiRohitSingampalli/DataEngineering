# Topic 28 — Kubernetes for Data Engineering

> Standalone deep-dive · Spark/Flink/Airflow/Argo on K8s + autoscaling + ops

[← Back to main](./README.md)

---

## How to use this document

Kubernetes is where modern data platforms run. If the target role runs on K8s (common at mid-to-large tech companies, less common at pure Snowflake/Databricks shops), expect probing on operators, scheduling, autoscaling, cost, and multi-tenancy. This chapter is for the K8s-native data platform engineer.

> **🧠 Principal Mindset:** Running Spark or Flink on K8s is not the same as running them on YARN. The abstractions, failure modes, and cost drivers are different. Mastering the K8s-specific patterns is the staff signal.

## Table of Contents

**PART I — FOUNDATIONS**
- 1. Why K8s for Data
- 2. K8s vs YARN
- 3. K8s Primitives Data Platforms Rely On

**PART II — SPARK ON K8S**
- 4. Spark on K8s Architecture
- 5. Spark Operator
- 6. Dynamic Allocation + Shuffle Service
- 7. Spark Connect

**PART III — FLINK ON K8S**
- 8. Flink Deployment Modes
- 9. Flink Kubernetes Operator
- 10. Flink Reactive Mode

**PART IV — AIRFLOW ON K8S**
- 11. KubernetesExecutor
- 12. KubernetesPodOperator
- 13. CeleryKubernetesExecutor Hybrid

**PART V — ARGO WORKFLOWS**
- 14. Argo on K8s
- 15. Argo Events
- 16. Argo for ML Pipelines

**PART VI — AUTOSCALING AND SCHEDULING**
- 17. Cluster Autoscaler + Karpenter
- 18. KEDA (Event-Driven Autoscaling)
- 19. Volcano Scheduler
- 20. Spot/Preemptible Nodes

**PART VII — OPERATIONS**
- 21. Multi-Tenancy on K8s
- 22. Observability
- 23. Cost Patterns

**PART VIII — INTERVIEW PRACTICE**
- 24. The Staff-Level K8s-for-Data Interview
- 25. Worked System Design
- 26. War Stories
- 27. Cheat Sheet &amp; Mental Models

---

## PART I — FOUNDATIONS

### 1. Why K8s for Data

```
DRIVERS:
   UNIFIED COMPUTE: Spark, Flink, Airflow, services, ML — one cluster
   PORTABILITY: same deployment on-prem, EKS, GKE, AKS
   DECLARATIVE: CRDs + operators = infra as code
   AUTOSCALING: Cluster Autoscaler, Karpenter, KEDA
   ISOLATION: namespaces, resource quotas, network policies

ALTERNATIVES:
   YARN: Hadoop-native; still exists in legacy
   Mesos: deprecated effectively
   Serverless: Databricks / Snowflake / Dataflow (zero infra for teams)
   VMs: direct bare provisioning; less flexibility

PATTERN IN 2026:
   Hyperscalers host K8s via EKS / GKE / AKS
   Data platforms (operators + services) deploy on it
   Teams get namespaces + quotas; platform team runs cluster
```

### 2. K8s vs YARN

```
YARN:
   ResourceManager + NodeManager + ApplicationMaster
   Built FOR Hadoop/Spark workloads
   Resource model: coarse (containers)
   
K8S:
   Scheduler + Kubelet + Controller Manager
   Generic: runs anything in a container
   Resource model: fine (CPU millicore + Mi memory requests/limits)

DIFFERENCES THAT MATTER FOR DATA:
   SHUFFLE: K8s needs External Shuffle Service for dynamic allocation (not native)
   NETWORK: K8s overlay networks add latency vs YARN direct
   SCHEDULING: K8s scheduler isn't optimized for gang scheduling; Volcano helps
   LOCAL SSDs: emptyDir on local SSDs; different from HDFS co-location
   PERMISSIONS: K8s RBAC + IAM mapping (IRSA on EKS, Workload Identity on GKE)
```

### 3. K8s Primitives Data Platforms Rely On

```
DEPLOYMENT / STATEFULSET: long-running services
JOB / CRONJOB: batch work; scheduled batch
POD: unit of scheduling
SERVICE: internal networking
CRD (Custom Resource Definition): extend K8s with new object types
   Spark Operator defines SparkApplication CRD
   Flink Operator defines FlinkDeployment CRD
   Argo Workflows defines Workflow CRD

OPERATORS: controllers that manage CRDs
   Watch for CRs; reconcile desired state
   For data platforms: handle driver/executor lifecycle, checkpoints, retries

SCHEDULING RELATED:
   Node affinity / anti-affinity
   Taints + tolerations (isolate workloads)
   Pod disruption budgets (safe eviction)
   Priority classes (which pods preempt others)
```

---

## PART II — SPARK ON K8S

### 4. Spark on K8s Architecture

```
SPARK ON K8S:
   
   Spark driver = K8s pod
   Spark executors = K8s pods (one per executor)
   Driver creates executor pods via K8s API (needs permissions)
   
   SUBMIT: spark-submit --master k8s://<api-server> ...
   
   DRIVER MODES:
      Client mode: driver runs OUTSIDE the cluster
      Cluster mode: driver runs INSIDE as a pod (standard)
```

### 5. Spark Operator

```
SPARK ON K8S OPERATOR (Google-originated; now Kubeflow):
   
   Defines SparkApplication CRD:
   
   apiVersion: sparkoperator.k8s.io/v1beta2
   kind: SparkApplication
   metadata:
     name: my-spark-job
   spec:
     type: Python
     mode: cluster
     image: myregistry/spark:3.5
     mainApplicationFile: s3://bucket/job.py
     driver:
       cores: 1
       memory: "4g"
       serviceAccount: spark-sa
     executor:
       instances: 10
       cores: 2
       memory: "8g"
     restartPolicy:
       type: Never
     sparkConf:
       spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem

Submit: kubectl apply -f spark-app.yaml
The operator creates driver + executor pods; monitors; cleans up.

PROS:
   Declarative Spark jobs
   Status visible in K8s (kubectl get sparkapp)
   Metrics + logging integration
```

### 6. Dynamic Allocation + Shuffle Service

```
DYNAMIC ALLOCATION: Spark adds / removes executors based on workload
   On YARN: Shuffle Service (per node) keeps shuffle data when executor dies
   On K8s: more complex — Spark 3.1+ supports dynamic alloc via two options:

   OPTION 1: EXTERNAL SHUFFLE SERVICE (K8s DaemonSet)
      Runs a shuffle service pod on each node
      Executors write shuffle data to local disk; service serves it
      Executor can die; data stays
      Complex to deploy
   
   OPTION 2: SHUFFLE TRACKING (Spark 3.1+)
      Spark knows which executors hold shuffle data
      Doesn't kill those executors even if idle
      Dynamic alloc partially effective
   
   OPTION 3: STATIC ALLOCATION
      Request fixed executors; no dynamic adjustment
      Simplest; works everywhere; may over-provision

PATTERN IN 2026:
   Static allocation for predictable batch
   Dynamic alloc with shuffle tracking for variable workloads
   External shuffle service in high-end deployments
```

### 7. Spark Connect

```
SPARK CONNECT (Spark 3.4+):
   
   Decouples client from Spark cluster via gRPC
   Client sends DataFrame operations; server executes
   Enables:
      Thin clients (notebooks, apps) without full Spark deps
      Longer-lived cluster shared across clients
      Language independence (Python, Scala, Rust, Go clients)

PATTERN:
   Spark Cluster on K8s, long-lived
   Spark Connect endpoint exposed as Service
   Notebooks / apps / Airflow use Spark Connect clients
   
Reduces cold-start overhead and shared-resource utilization.
```

---

## PART III — FLINK ON K8S

### 8. Flink Deployment Modes

```
FLINK DEPLOYMENT MODES on K8s:
   
   SESSION: long-running Flink cluster; submit multiple jobs to it
      Good for: many small jobs; interactive
      Risk: one job failure affects others
   
   PER-JOB: one Flink cluster per job
      Good for: isolated production jobs
      Risk: slow startup per job
   
   APPLICATION: single Spark/Flink application per cluster, main() runs IN cluster
      Standard pattern in 2024+
      Each job gets its own cluster but main() isn't in the client

For production streaming: APPLICATION mode via Flink Kubernetes Operator.
```

### 9. Flink Kubernetes Operator

```
FLINK KUBERNETES OPERATOR (Apache; official in 2022+):
   
   Defines FlinkDeployment CRD:
   
   apiVersion: flink.apache.org/v1beta1
   kind: FlinkDeployment
   metadata:
     name: my-streaming-job
   spec:
     image: flink:1.18
     flinkVersion: v1_18
     flinkConfiguration:
       taskmanager.numberOfTaskSlots: "2"
     jobManager:
       resource:
         memory: "2048m"
         cpu: 1
     taskManager:
       resource:
         memory: "4096m"
         cpu: 2
     job:
       jarURI: s3://bucket/job.jar
       parallelism: 10
       upgradeMode: savepoint

CAPABILITIES:
   Lifecycle: start, update, suspend, delete
   Upgrades via savepoint (stop, snapshot, redeploy from snapshot)
   HA via K8s (jobmanager leader election through K8s ConfigMap)
   Autoscaling (reactive or adaptive, see 10)
```

### 10. Flink Reactive Mode

```
REACTIVE MODE (Flink 1.13+):
   Flink job automatically scales parallelism based on available resources
   Combine with K8s autoscaling: add TaskManagers → Flink uses them
   
   Trigger scaling:
      Horizontal Pod Autoscaler (HPA) on TaskManager Deployment based on CPU
      Or custom metrics (lag, backlog)
   
   LIMITATIONS:
      Scaling events require stop-with-savepoint + restart (brief pause)
      Not for truly real-time (ms-latency) workloads
      
AUTOSCALER (Flink 1.17+ via operator):
   Metric-based scaling (backpressure, lag)
   Scales operator parallelism independently
   More sophisticated than reactive mode
```

---

## PART IV — AIRFLOW ON K8S

### 11. KubernetesExecutor

```
AIRFLOW KUBERNETESEXECUTOR:
   Each task runs as a K8s Pod
   Pod created on task start; cleaned up on task finish
   No long-running workers; no Celery broker needed

PROS:
   Elastic: scale to zero when no tasks running
   Isolation: each task in its own pod
   Resource per-task: specify CPU/memory per operator

CONS:
   Pod startup overhead (seconds) adds latency per task
   High task volume = many pod lifecycle events
```

### 12. KubernetesPodOperator

```
KUBERNETESPODOPERATOR: run arbitrary container as an Airflow task
   
   from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

   task = KubernetesPodOperator(
       task_id="run_data_job",
       image="myregistry/data-job:v1",
       cmds=["python", "run.py"],
       arguments=["--date", "{{ ds }}"],
       resources={"request_memory": "4Gi", "request_cpu": "2"},
       secrets=[Secret("env", "DB_URL", "db-creds", "url")],
       in_cluster=True,
   )

Use when: tasks need isolated images / specific resource profiles.
```

### 13. CeleryKubernetesExecutor Hybrid

```
CELERYKUBERNETESEXECUTOR:
   For most tasks: Celery workers (always on; fast startup)
   For heavy tasks: route to KubernetesExecutor (dedicated pod per task)

Trade-off: operational complexity + Celery broker required
Good for: high-volume mix of lightweight + heavy tasks
```

---

## PART V — ARGO WORKFLOWS

### 14. Argo on K8s

```
ARGO WORKFLOWS: K8s-native workflow engine (see 26-orchestration-alternatives).

Each step = container (K8s pod).
Workflows are K8s CRDs: 
   apiVersion: argoproj.io/v1alpha1
   kind: Workflow
   spec:
     templates: ...

For ML / batch / infra pipelines on K8s, Argo often wins over Airflow:
   • Native to K8s (no sidecar scheduler)
   • Massive parallelism (10K+ containers)
   • DAGs as declarative YAML
```

### 15. Argo Events

```
ARGO EVENTS: event-driven workflow triggers.
   
   Event sources: Kafka, S3, webhooks, Pub/Sub, GitHub, etc.
   Sensors: listen for events; trigger workflows
   
   Pattern:
      S3 PutObject → EventSource → Sensor → Argo Workflow

Enables event-driven K8s-native pipelines without a separate orchestrator.
```

### 16. Argo for ML Pipelines

```
ARGO + MLOPS:
   Kubeflow Pipelines built on top of Argo Workflows
   Each step = container (training, eval, model push)
   GPU scheduling via K8s node selectors
   Massive parallelism for hyperparameter sweeps

Vertex AI Pipelines (GCP) uses a similar pattern (Argo-inspired).
```

---

## PART VI — AUTOSCALING AND SCHEDULING

### 17. Cluster Autoscaler + Karpenter

```
CLUSTER AUTOSCALER (CA):
   K8s-standard component
   Adds nodes when pods are PENDING due to insufficient resources
   Removes nodes when underutilized
   Works per node group (ASG on AWS, MIG on GCP)
   
KARPENTER (AWS-first; 2022+):
   Direct provisioning: skips node groups; picks instance types dynamically
   Faster scale-up than CA (seconds vs minutes)
   Right-sizes nodes to pod requirements
   AWS-native but K8s-standard; EKS-tightly-integrated
   
   GCP/Azure equivalents emerging (gke-autopilot, aks node autoprovisioner)

STAFF SIGNAL: Karpenter + Spot = modern cost-optimized K8s data plane.
```

### 18. KEDA (Event-Driven Autoscaling)

```
KEDA (Kubernetes Event-Driven Autoscaler):
   Scales workloads based on external event sources
   Not just CPU/memory: Kafka lag, SQS queue depth, Postgres rows, Prometheus metrics
   
   Example: scale Flink TaskManagers based on Kafka consumer lag
   
   apiVersion: keda.sh/v1alpha1
   kind: ScaledObject
   metadata:
     name: flink-tm-scaler
   spec:
     scaleTargetRef:
       name: flink-taskmanager
     triggers:
       - type: kafka
         metadata:
           bootstrapServers: ...
           consumerGroup: my-job
           topic: events
           lagThreshold: "10000"

KEDA is the right tool for data-pipeline-driven scaling.
```

### 19. Volcano Scheduler

```
VOLCANO: K8s batch scheduler (CNCF).
   
   Addresses K8s default scheduler weaknesses for data workloads:
      GANG SCHEDULING: schedule all pods of a job together (or none)
         K8s default scheduler is pod-by-pod → can deadlock on multi-pod jobs
         Volcano: schedule all N pods atomically
      
      JOB QUEUES: multi-tenant queues with weights + preemption
      
      FAIR-SHARE scheduling across tenants
      
      PRIORITY classes with preemption

Use when: running Spark / Flink / ML workloads with many pods per job
   on shared K8s clusters.
```

### 20. Spot/Preemptible Nodes

```
SPOT (AWS), PREEMPTIBLE (GCP), SPOT (Azure):
   Up to 90% discount
   Can be reclaimed in 1-2 minutes

PATTERNS FOR DATA:
   SPARK: executors on spot (OK — Spark reruns lost tasks); driver on on-demand
   FLINK: streaming jobs on spot possible with careful checkpointing; 
          not for low-latency SLAs
   AIRFLOW: Celery/KubernetesExecutor pods on spot OK; webserver/scheduler on on-demand
   AIRFLOW TASKS: tolerance for restart on spot preemption required (retry)

SAVINGS: often 50-70% cluster cost reduction for batch workloads.

STAFF SIGNAL: argue the WORKLOAD SHAPE that tolerates preemption.
   Senior answer: "we use spot". Staff answer: "we use spot for executors
   with retries, split spot/on-demand by task criticality, and run
   stateful components only on on-demand with EBS-backed PVCs."
```

---

## PART VII — OPERATIONS

### 21. Multi-Tenancy on K8s

```
NAMESPACE-PER-TEAM (default):
   Each team gets a namespace
   RBAC + ResourceQuota + LimitRange per namespace
   Network policies for isolation

PROBLEMS AT SCALE:
   Noisy neighbors (CPU + memory contention at node level)
   Shared control plane limits (etcd, API server throughput)
   Cross-namespace service discovery complexity

PATTERNS:
   DEDICATED NODE POOLS per team (via taints + tolerations)
   VIRTUAL CLUSTERS (vcluster, Capsule): per-team "virtual" K8s inside a shared one
   MULTI-CLUSTER FEDERATION: one cluster per team (operational cost)

STAFF SIGNAL: multi-tenancy is HARD. Argue the specific isolation
   requirements (compliance, performance SLAs, cost allocation) before
   picking the pattern.
```

### 22. Observability

```
OBSERVABILITY STACK (K8s-native data platforms):
   
   METRICS: Prometheus + Grafana
      Spark operator exposes metrics; Flink has Prometheus reporter
      Pod/node metrics via cAdvisor + kubelet
      Custom metrics via /metrics endpoints
   
   LOGS: Loki or Elasticsearch
      Fluent Bit / Fluent Bit DaemonSet forwards container logs
   
   TRACES: Jaeger / Tempo (less common for pure data; more for services)
   
   EVENTS: K8s events via kube-state-metrics
      Alert on OOMKilled, FailedScheduling, Evicted

DATA-SPECIFIC:
   Spark History Server for completed jobs
   Flink JobManager Web UI (plus Prometheus metrics)
   Airflow metrics via statsd exporter
   Argo UI for workflows
```

### 23. Cost Patterns

```
TOP K8s COST LEVERS FOR DATA:
   1. Spot/preemptible for batch workloads (50-70% save)
   2. Karpenter for right-sized nodes
   3. Cluster Autoscaler scale-down (avoid idle nodes)
   4. Resource request/limit tuning (over-request = waste)
   5. Namespace-level cost attribution via tools (Kubecost, OpenCost)
   6. GPU scheduling + fractional GPUs for ML
   7. Shut down dev / staging during off-hours
   8. Spot interruption handling via node termination handlers
   9. Taints + tolerations to pack workloads tightly
   10. Right-size cluster (don't over-provision for peak + margin)

COST ATTRIBUTION:
   Kubecost / OpenCost: namespace + label-based cost allocation
   Cloud native: EKS CloudWatch; GKE cost explorer
```

---

## PART VIII — INTERVIEW PRACTICE

### 24. The Staff-Level K8s-for-Data Interview

```
COMMON QUESTIONS:
   1. "Spark on K8s vs YARN."
   2. "Design a data platform on K8s for 500 engineers."
   3. "How does Flink HA work on K8s?"
   4. "Dynamic allocation on K8s: how + why complicated."
   5. "Multi-tenancy on K8s for data."
   6. "KEDA vs HPA for streaming workloads."
   7. "Spot instance strategy for Spark."
   8. "Cluster autoscaler vs Karpenter."
   9. "Argo Workflows vs Airflow for ML pipelines."
   10. "Observability on K8s data platforms."
```

### 25. Worked System Design

```
Q: Build a shared data platform on K8s for 200 data engineers +
   100 ML engineers across 30 teams. 500 TB Delta Lake. 
   Workloads: Spark ETL, Flink streaming, Airflow orchestration,
   ML training. Budget flexibility ($200-400K/month).

STAFF ANSWER:

CLUSTER:
   EKS clusters: one per environment (dev, staging, prod)
   Prod: 3 AZs; multi-node-group (on-demand, spot, GPU)
   Karpenter for autoscaling + right-sizing
   Volcano scheduler for gang scheduling of Spark/Flink

NAMESPACES:
   Namespace per team
   ResourceQuota + LimitRange per namespace
   NetworkPolicies for isolation
   RBAC via SSO (OIDC)
   Dedicated node pools for sensitive teams (via taints/tolerations)

COMPUTE WORKLOADS:
   Spark:
      Spark Operator for ETL jobs
      Jobs submitted via Airflow KubernetesPodOperator or Argo
      Executors on spot (with retries); drivers on on-demand
      Spark Connect for shared interactive clusters
   
   Flink:
      Flink Kubernetes Operator
      Application mode for production streams
      KEDA for Kafka-lag-based autoscaling
      HA via K8s ConfigMap-based leader election
   
   ML Training:
      Argo Workflows for pipeline orchestration
      GPU node pool (p4d / p5 instances)
      KubeRay for distributed training on Ray
   
   Airflow:
      KubernetesExecutor for isolation per task
      KubernetesPodOperator for custom images

STORAGE:
   S3 for data lake (Delta Lake)
   EFS for shared read-write where needed (rare)
   EBS PVCs for stateful components (Flink HA, Postgres)
   Local SSDs (NVMe) for shuffle on heavy Spark nodes

AUTOSCALING:
   Karpenter: scales nodes based on pod demand
   Spot pools: 70% of batch compute
   KEDA: scales Flink TaskManagers based on Kafka lag
   HPA on long-running services (API gateways, metadata services)

OBSERVABILITY:
   Prometheus + Grafana for metrics
   Loki for logs; log retention 30 days hot
   Jaeger for traces (API services)
   Spark History Server + Flink UI + Argo UI
   PagerDuty integration via Alertmanager

COST ATTRIBUTION:
   Kubecost per namespace
   Billing exports to warehouse for finance
   Team dashboards with monthly spend trends

GOVERNANCE:
   Image registry: ECR; images scanned on push
   OPA / Kyverno for policy-as-code (block privileged containers, enforce tags)
   Secrets via External Secrets Operator (sourced from AWS Secrets Manager)
   Service mesh (Istio or Linkerd) for mTLS between services

TOTAL COST:
   EKS control plane: $500/month per cluster × 3 = $1.5K/month
   Compute (mix of on-demand + spot): $200K/month
   GPU compute: $80K/month
   EBS + EFS: $10K/month
   S3: already accounted in data lake budget
   Observability (self-hosted): $15K/month in compute
   Platform team ops: ~$80K/month in labor
   Total: ~$300K/month

ALTERNATIVES CONSIDERED:
   Managed Databricks: faster, but higher cost at this scale + lock-in
   Serverless (EMR Serverless, Dataproc Serverless): great for burst, 
      but less flexible for streaming + ML
   Mesos: deprecated
```

### 26. War Stories

> **🔥 Production Scenario — The Shuffle Service Oversight.** A team ran Spark on K8s with dynamic allocation but no external shuffle service. Executors scaled down → lost shuffle data → stages re-ran. Jobs 2-3x slower. Fix: enabled shuffle tracking in Spark 3.1+ or deployed external shuffle service as DaemonSet. Lesson: dynamic allocation on K8s needs explicit shuffle planning.

> **🔥 Production Scenario — The Volcano Save.** A team ran Spark jobs on K8s with default scheduler. Multiple concurrent Spark jobs deadlocked (each got some pods but not enough to start). Switched to Volcano gang scheduler: atomic multi-pod scheduling. Deadlock eliminated. Lesson: K8s default scheduler isn't built for multi-pod data jobs.

> **🔥 Production Scenario — The Spot Interruption Nightmare.** A team ran all Spark workloads on spot. Spot interruption rate was 5-10% per hour during peak demand. Jobs failed repeatedly before completing. Fix: moved drivers to on-demand; added task retry; set spot pool to lower-demand instance types + capacity-optimized allocation. Interruption rate dropped to 1-2%; jobs completed reliably. Lesson: spot strategy needs instance-type and allocation-strategy tuning, not just "use spot."

> **🔥 Production Scenario — The Karpenter 10x Scale.** A team used Cluster Autoscaler with ASG-based node groups. Scale-up: 3-5 minutes for new nodes. Jobs waited; batch windows missed. Migrated to Karpenter: scale-up in 30-60 seconds; right-sized instance types (not just pre-configured ASGs). Batch windows met reliably. Lesson: Karpenter's dynamic provisioning materially improves data workload performance.

> **🔥 Production Scenario — The Resource Request Trap.** A team requested 16 GB per Spark executor "just to be safe." Actual usage: 6 GB. 10x over-provisioning. Node utilization 30%. Right-sized to 8 GB; node utilization rose to 70%; cost dropped 40%. Lesson: resource requests are quotas; they count against capacity; over-requesting is waste.

### 27. Cheat Sheet &amp; Mental Models

#### 27.1 One-Minute Pitch

```
K8s for data = unified compute via operators + CRDs.
   Spark on K8s via Spark Operator
   Flink on K8s via Flink Operator
   Airflow on K8s via KubernetesExecutor
   Argo Workflows for K8s-native pipelines

Autoscaling: Karpenter + Cluster Autoscaler + KEDA + Volcano
Spot/preemptible for batch; on-demand for drivers + stateful
Multi-tenancy via namespaces + ResourceQuota + dedicated node pools
Observability via Prometheus + Grafana + Loki

STAFF SIGNAL: argue the TRADE-OFFS specific to K8s vs YARN or managed
   services. Pick K8s only when control + portability justifies ops.
```

#### 27.2 Senior vs Staff

| Senior | Staff |
|--------|-------|
| "Spark on K8s." | "Spark Operator with Spark 3.5; dynamic allocation via shuffle tracking (not external shuffle service — simpler); drivers on on-demand, executors on spot via Karpenter; local NVMe for shuffle; Volcano scheduler for gang scheduling." |
| "Use autoscaling." | "Three autoscalers: Karpenter for node provisioning (right-sized, fast), HPA for stateless services, KEDA for data-pipeline-driven scaling (Kafka lag, queue depth). Each layer has distinct responsibility; CA alone is insufficient for data workloads." |
| "Spot saves money." | "Spot for batch executors with retries; on-demand for drivers and stateful components (Flink JobManagers, Postgres). Spot strategy depends on instance-type diversification and capacity-optimized allocation to minimize interruption rate." |
| "Multi-tenancy via namespaces." | "Namespaces + ResourceQuota + LimitRange + NetworkPolicies for ordinary isolation; dedicated node pools (via taints/tolerations) for compliance tenants; virtual clusters (vcluster) for teams needing full K8s control. Isolation level matches the tenant class." |

> **🧠 Principal Mindset:** Running data platforms on K8s is a continuous ops investment. Benefits: portability, unified tooling, declarative infra. Costs: operator maintenance, autoscaler tuning, scheduler quirks, multi-tenancy complexity. The staff question is "does the control/portability benefit exceed the ops cost given the org's scale and skill?" At <50 engineers, managed services are often better. At 500+ engineers, K8s unification often wins.

---

[← Back to main](./README.md)
