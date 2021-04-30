package sort;

import java.util.Arrays;

/**
 * 1.构建初始堆，将待排序列构成一个大顶堆(或者小顶堆)，升序大顶堆，降序小顶堆；
 * 2.将堆顶元素与堆尾元素交换，并断开(从待排序列中移除)堆尾元素。
 * 3.重新构建堆。
 * 4.重复2~3，直到待排序列中只剩下一个元素(堆顶元素)。
 */
public class HeapSort {
    public static void main(String[] args) {
        int[] arr = {16, 7, 3, 20, 17, 8};
        heapSort(arr);
        System.out.println(Arrays.toString(arr));
    }

    public static void heapSort(int[] arr) {
        //创建堆
        for (int i = (arr.length - 1) / 2; i >= 0; i--) {
            //从第一个非叶子节点从下至上，从右至左调整结构
            adjustHeap(arr, i, arr.length);
        }
        //调整堆结构+交换堆顶元素与末尾元素
        for (int i = arr.length - 1; i > 0; i--) {
            //将堆顶元素与末尾元素进行交换
            int tmp = arr[i];
            arr[i] = arr[0];
            arr[0] = tmp;

            //重新对堆进行调整
            adjustHeap(arr, 0, i);
        }
    }

    /**
     * 调整堆
     *
     * @param arr    待排序列
     * @param parent 父节点
     * @param length 待排序列尾元素索引
     */
    public static void adjustHeap(int[] arr, int parent, int length) {
        int temp = arr[parent];
        int lChild = 2 * parent + 1;
        while (lChild < length) {
            int rChild = lChild + 1;
            // 如果有右孩子结点，并且右孩子结点的值大于左孩子结点，则选取右孩子结点
            if (rChild < length && arr[lChild] < arr[rChild]) {
                lChild++;
            }
            // 如果父结点的值已经大于孩子结点的值，则直接结束
            if (temp >= arr[lChild]) {
                break;
            }
            // 把孩子结点的值赋给父结点
            arr[parent] = arr[lChild];

            //选取孩子结点的左孩子结点,继续向下筛选
            parent = lChild;
            lChild = 2 * lChild + 1;
        }
        arr[parent] = temp;
    }
}
