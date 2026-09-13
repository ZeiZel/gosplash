{{/*
fullname — .Release.Name, БЕЗ примеси имени чарта (см. подробный разбор
компромисса в deploy/helm/media/templates/_helpers.tpl). mk/k8s.mk
устанавливает этот чарт релизом "thumbnail-worker".
*/}}
{{- define "thumbnail-worker.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "thumbnail-worker.labels" -}}
app.kubernetes.io/name: thumbnail-worker
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "thumbnail-worker.selectorLabels" -}}
app.kubernetes.io/name: thumbnail-worker
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "thumbnail-worker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "thumbnail-worker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
