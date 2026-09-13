{{/*
НЕОЧЕВИДНОЕ РЕШЕНИЕ: fullname — это .Release.Name, БЕЗ примеси имени чарта
и без обрезки/хеширования (как делает стандартный `helm create`). В этом
проекте на namespace всегда ровно один экземпляр каждого сервиса — не
предполагается ставить order дважды под разными релизами в одном
неймспейсе. Предсказуемое имя здесь важно в ОБРАТНУЮ сторону: order сам
собирает адреса wallet-grpc/catalog-grpc из values.yaml (WALLET_GRPC_TARGET,
CATALOG_GRPC_TARGET), полагаясь на то, что ЧУЖИЕ релизы называются "wallet"
и "catalog" — это работает только тогда, когда релиз order-service тоже
ставится под фиксированным именем (см. mk/k8s.mk: helm upgrade --install
order ...), а не auto-generated release name.
Платим за это тем, что нельзя поставить второй релиз того же чарта рядом
без конфликта имён — для этого проекта это приемлемое ограничение.
*/}}
{{- define "order.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "order.labels" -}}
app.kubernetes.io/name: order
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "order.selectorLabels" -}}
app.kubernetes.io/name: order
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "order.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "order.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
