{{/*
НЕОЧЕВИДНОЕ РЕШЕНИЕ: fullname — это .Release.Name, БЕЗ примеси имени чарта
и без обрезки/хеширования (как делает стандартный `helm create`). В этом
проекте на namespace всегда ровно один экземпляр каждого сервиса — не
предполагается ставить analytics дважды под разными релизами в одном
неймспейсе, — а предсказуемое имя понадобится, как только у AnalyticsService
(TopPhotos/PhotoStats) появится внутренний вызывающий (сегодня в проекте
такого нет ни у одного сервиса — витрина отчётов вызывается только
напрямую grpcurl/будущим gateway) или когда gRPC-gateway станет проксировать
сюда снаружи. Headless Service заводится уже сейчас, по общему правилу
ADR 0015 "каждый gRPC-сервис — headless", а не по факту существующего
трафика — тогда и фиксированное имя релиза ("analytics", см. mk/k8s.mk)
сразу окажется тем, что ожидает клиент.
Платим за это тем, что нельзя поставить второй релиз того же чарта рядом
без конфликта имён — для этого проекта это приемлемое ограничение.
*/}}
{{- define "analytics.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "analytics.labels" -}}
app.kubernetes.io/name: analytics
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "analytics.selectorLabels" -}}
app.kubernetes.io/name: analytics
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "analytics.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "analytics.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
