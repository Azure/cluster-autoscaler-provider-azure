/*
Copyright (c) Microsoft Corporation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"context"
	"os"
	"strings"

	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (e *Environment) prepareGeneral(ctx context.Context) error {
	driver, err := os.ReadFile(repoFile("test/hack/dra-driver.yaml.in"))
	if err != nil {
		return err
	}

	if err := e.command(ctx, strings.ReplaceAll(string(driver), "@RUN_ID@", e.Config.RunID), "kubectl", "apply", "-f", "-"); err != nil {
		return err
	}

	if err := e.command(
		ctx,
		"",
		"kubectl",
		"-n",
		"kube-system",
		"rollout",
		"status",
		"daemonset/dra-example-driver-kubeletplugin",
		"--timeout=6m",
	); err != nil {
		return err
	}

	for _, priority := range []struct {
		name  string
		value int32
	}{{"expendable", -15}, {"high", 1000}} {
		if err := e.K8s.Create(ctx, &schedulingv1.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{Name: e.Config.RunID + "-" + priority.name}, Value: priority.value,
		}); err != nil {
			return err
		}
	}

	return nil
}
