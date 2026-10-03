{{- define "pondra.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}{{ .Release.Name | trunc 50 | trimSuffix "-" }}{{- else -}}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 50 | trimSuffix "-" }}{{- end -}}
{{- end -}}

{{- define "pondra.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "pondra.selector" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* The lake is in a bucket (every node reads it there) or a folder. */}}
{{- define "pondra.bucket" -}}
{{- if regexMatch "^(s3|gs|az|abfs|abfss)://" .Values.lake -}}true{{- end -}}
{{- end -}}

{{/* What a node opens: the bucket, or the folder on its volume (/data/lake). */}}
{{- define "pondra.lake" -}}
{{- if include "pondra.bucket" . -}}{{ .Values.lake }}{{- else -}}/data/lake{{- end -}}
{{- end -}}

{{/* The lake can be shared by several nodes and readers. */}}
{{- define "pondra.shared" -}}
{{- if or (include "pondra.bucket" .) .Values.sharedStorage.existingClaim -}}true{{- end -}}
{{- end -}}

{{- define "pondra.replicas" -}}
{{- if kindIs "invalid" .Values.replicas -}}{{ if include "pondra.shared" . }}3{{ else }}1{{ end }}{{- else -}}{{ .Values.replicas }}{{- end -}}
{{- end -}}

{{- define "pondra.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{ .Values.image.repository }}:{{ $tag }}{{ if .Values.python.enabled }}-python{{ end }}
{{- end -}}

{{- define "pondra.serviceAccount" -}}
{{- if .Values.serviceAccount.create -}}{{ .Values.serviceAccount.name | default (include "pondra.fullname" .) }}{{- else -}}{{ .Values.serviceAccount.name | default "default" }}{{- end -}}
{{- end -}}

{{- define "pondra.tokensSecret" -}}{{ .Values.auth.existingSecret | default (printf "%s-tokens" (include "pondra.fullname" .)) }}{{- end -}}
{{- define "pondra.keySecret" -}}{{ .Values.secretKey.existingSecret | default (printf "%s-key" (include "pondra.fullname" .)) }}{{- end -}}
{{- define "pondra.scheme" -}}{{ if .Values.tls.existingSecret }}HTTPS{{ else }}HTTP{{ end }}{{- end -}}

{{/* What can't work, refused before anything is made. */}}
{{- define "pondra.validate" -}}
{{- $shared := include "pondra.shared" . -}}
{{- if and (gt (int (include "pondra.replicas" .)) 1) (not $shared) -}}
{{- fail "Several nodes share one lake: give lake (s3://bucket/prefix, gs://…, az://…) or sharedStorage.existingClaim (a ReadWriteMany claim), or replicas=1." -}}
{{- end -}}
{{- if and (gt (int .Values.readers.replicas) 0) (not $shared) -}}
{{- fail "Readers read the nodes' lake: give lake (a bucket) or sharedStorage.existingClaim." -}}
{{- end -}}
{{- if and (not $shared) (not .Values.persistence.enabled) -}}
{{- fail "The lake would be lost with the pod: keep persistence.enabled, or give lake (a bucket) or sharedStorage.existingClaim." -}}
{{- end -}}
{{- if and .Values.python.enabled (not .Values.auth.enabled) -}}
{{- fail "python.enabled runs code on the nodes, which needs tokens (auth.enabled)." -}}
{{- end -}}
{{- if lt (int (include "pondra.replicas" .)) 1 -}}
{{- fail "replicas: at least 1 node leads." -}}
{{- end -}}
{{- end -}}

{{/* A node's or a reader's container: everything but its own flags. */}}
{{- define "pondra.container" -}}
image: {{ include "pondra.image" . }}
imagePullPolicy: {{ .Values.image.pullPolicy }}
securityContext: {{- toYaml .Values.securityContext | nindent 2 }}
env:
  - {name: POD_NAME, valueFrom: {fieldRef: {fieldPath: metadata.name}}}
  - {name: POD_NAMESPACE, valueFrom: {fieldRef: {fieldPath: metadata.namespace}}}
  - {name: POD_IP, valueFrom: {fieldRef: {fieldPath: status.podIP}}}
  - {name: PONDRA_SECRET_KEY, valueFrom: {secretKeyRef: {name: {{ include "pondra.keySecret" . }}, key: PONDRA_SECRET_KEY}}}
  # (a stopping pod says it isn't ready, then waits this long before turning requests away: the
  # Service takes a moment to stop sending it any)
  - {name: PONDRA_DRAIN_GRACE_SECS, value: "5"}
  {{- if .Values.auth.enabled }}
  {{- range list "PONDRA_ADMIN_TOKEN" "PONDRA_WRITE_TOKEN" "PONDRA_READ_TOKEN" }}
  - {name: {{ . }}, valueFrom: {secretKeyRef: {name: {{ include "pondra.tokensSecret" $ }}, key: {{ . }}, optional: true}}}
  {{- end }}
  {{- end }}
  {{- if .Values.tls.existingSecret }}
  - {name: PONDRA_TLS_CERT, value: /tls/tls.crt}
  - {name: PONDRA_TLS_KEY, value: /tls/tls.key}
  {{- if .Values.tls.mutual }}
  - {name: PONDRA_TLS_CA, value: /tls/ca.crt}
  {{- end }}
  {{- end }}
  {{- range $k, $v := .Values.bucket.env }}
  - {name: {{ $k }}, value: {{ $v | quote }}}
  {{- end }}
  {{- with .Values.extraEnv }}{{ toYaml . | nindent 2 }}{{- end }}
{{- if or .Values.bucket.existingSecret .Values.extraEnvFrom }}
envFrom:
  {{- with .Values.bucket.existingSecret }}
  - secretRef: {name: {{ . }}}
  {{- end }}
  {{- with .Values.extraEnvFrom }}{{ toYaml . | nindent 2 }}{{- end }}
{{- end }}
ports:
  - {name: http, containerPort: 8080}
  {{- if .Values.postgres.enabled }}
  - {name: postgres, containerPort: 5432}
  {{- end }}
  {{- if .Values.flight.enabled }}
  - {name: flight, containerPort: 8815}
  {{- end }}
  {{- if .Values.kafka.enabled }}
  - {name: kafka, containerPort: 9092}
  {{- end }}
# (the port opens once the lake is open; a cold start on a big lake can take a while. Ready: caught
# up with its leader, not stopping, and able to reach the bucket. Alive: the process answers.)
startupProbe:
  httpGet: {path: /healthz, port: http, scheme: {{ include "pondra.scheme" . }}}
  periodSeconds: 2
  failureThreshold: 300
readinessProbe:
  httpGet: {path: /ready, port: http, scheme: {{ include "pondra.scheme" . }}}
  periodSeconds: 5
  failureThreshold: 2
livenessProbe:
  httpGet: {path: /healthz, port: http, scheme: {{ include "pondra.scheme" . }}}
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 6
volumeMounts:
  - {name: data, mountPath: /data}
  - {name: tmp, mountPath: /tmp}
  {{- if .Values.sharedStorage.existingClaim }}
  - {name: lake, mountPath: /data/lake, subPath: {{ .Values.sharedStorage.subPath | quote }}}
  {{- end }}
  {{- if .Values.tls.existingSecret }}
  - {name: tls, mountPath: /tls, readOnly: true}
  {{- end }}
  {{- with .Values.extraVolumeMounts }}{{ toYaml . | nindent 2 }}{{- end }}
{{- end -}}

{{/* The flags every node and reader takes. */}}
{{- define "pondra.flags" -}}
- --addr
- 0.0.0.0:8080
{{- if .Values.postgres.enabled }}
- --pg
- 0.0.0.0:5432
{{- end }}
{{- if .Values.flight.enabled }}
- --flight
- 0.0.0.0:8815
{{- end }}
{{- if .Values.python.enabled }}
- --python
- auto
{{- end }}
{{- with .Values.extraArgs }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* The pod's volumes, besides its /data. */}}
{{- define "pondra.volumes" -}}
- {name: tmp, emptyDir: {}}
{{- with .Values.sharedStorage.existingClaim }}
- {name: lake, persistentVolumeClaim: {claimName: {{ . }}}}
{{- end }}
{{- with .Values.tls.existingSecret }}
- {name: tls, secret: {secretName: {{ . }}, defaultMode: 0440}}
{{- end }}
{{- with .Values.extraVolumes }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "pondra.podSpec" -}}
serviceAccountName: {{ include "pondra.serviceAccount" . }}
automountServiceAccountToken: false
securityContext: {{- toYaml .Values.podSecurityContext | nindent 2 }}
{{- with .Values.imagePullSecrets }}
imagePullSecrets: {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.priorityClassName }}
priorityClassName: {{ . }}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector: {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations: {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.topologySpreadConstraints }}
topologySpreadConstraints: {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* Labels or annotations given in values, every value a string (`--set podAnnotations.round=2` is a number, which Kubernetes refuses). */}}
{{- define "pondra.strings" -}}
{{- range $k, $v := . }}
{{ $k }}: {{ $v | toString | quote }}
{{- end }}
{{- end -}}
