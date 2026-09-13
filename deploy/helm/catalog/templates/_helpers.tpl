{{/*
НЕОЧЕВИДНОЕ РЕШЕНИЕ: fullname — это .Release.Name, БЕЗ примеси имени чарта
и без обрезки/хеширования (как делает стандартный `helm create`). В этом
проекте на namespace всегда ровно один экземпляр каждого сервиса — не
предполагается ставить catalog дважды под разными релизами в одном
неймспейсе, — а предсказуемое имя нужно ДРУГИМ чартам: order обращается
к catalog по headless-DNS "catalog-grpc" (см. values.yaml order:
CATALOG_GRPC_TARGET), и это работает только тогда, когда релиз реально
называется "catalog" (см. mk/k8s.mk: helm upgrade --install catalog ...).
Платим за это тем, что нельзя поставить второй релиз того же чарта рядом
без конфликта имён — для этого проекта это приемлемое ограничение.
*/}}
{{- define "catalog.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "catalog.labels" -}}
app.kubernetes.io/name: catalog
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "catalog.selectorLabels" -}}
app.kubernetes.io/name: catalog
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "catalog.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "catalog.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
