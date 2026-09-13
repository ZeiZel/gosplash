{{/*
НЕОЧЕВИДНОЕ РЕШЕНИЕ: fullname — это .Release.Name, БЕЗ примеси имени чарта
и без обрезки/хеширования. В этом проекте на namespace всегда ровно один
экземпляр каждого сервиса, поэтому в mk/k8s.mk order-worker всегда
устанавливается фиксированным именем релиза "order-worker" (helm upgrade
--install order-worker ...) — предсказуемость здесь не нужна ДРУГИМ чартам
(в отличие от media/catalog/wallet, к которым кто-то ходит по headless-DNS):
у order-worker вообще нет ни Service, ни входящего трафика (см. ниже,
templates/deployment.yaml), поэтому единообразие фикс-имени — просто общий
стиль репозитория, а не техническая необходимость.
*/}}
{{- define "order-worker.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "order-worker.labels" -}}
app.kubernetes.io/name: order-worker
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "order-worker.selectorLabels" -}}
app.kubernetes.io/name: order-worker
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "order-worker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "order-worker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
