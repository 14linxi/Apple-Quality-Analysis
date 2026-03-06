// 自定义JavaScript文件

// 当文档加载完成时执行
document.addEventListener('DOMContentLoaded', function() {
    
    // 为所有卡片添加动画效果
    const cards = document.querySelectorAll('.card');
    cards.forEach(card => {
        card.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-10px)';
            this.style.boxShadow = '0 10px 30px rgba(0, 0, 0, 0.15)';
        });
        
        card.addEventListener('mouseleave', function() {
            this.style.transform = 'translateY(-5px)';
            this.style.boxShadow = '0 8px 25px rgba(0, 0, 0, 0.1)';
        });
    });
    
    // 自动关闭警告提示
    const alerts = document.querySelectorAll('.alert');
    alerts.forEach(alert => {
        setTimeout(function() {
            const closeButton = alert.querySelector('.btn-close');
            if (closeButton) {
                closeButton.click();
            }
        }, 5000); // 5秒后自动关闭
    });
    
    // 如果存在预测表单，添加交互效果
    const predictionForm = document.querySelector('form[action*="prediction"]');
    if (predictionForm) {
        // 确保模型选择框显示正确的选中值
        const modelSelect = predictionForm.querySelector('#model');
        if (modelSelect) {
            // 获取默认值
            const defaultValue = modelSelect.getAttribute('data-default-value');
            if (defaultValue) {
                // 设置选择框的值
                modelSelect.value = defaultValue;
                
                // 触发 change 事件，确保任何关联的 UI 都更新
                const event = new Event('change');
                modelSelect.dispatchEvent(event);
            }
        }

        // 为数字输入框添加滑块效果
        const numberInputs = predictionForm.querySelectorAll('input[type="number"]');
        numberInputs.forEach(input => {
            const min = parseFloat(input.getAttribute('min') || '-5');
            const max = parseFloat(input.getAttribute('max') || '5');
            const step = parseFloat(input.getAttribute('step') || '0.1');
            
            // 创建滑块容器
            const sliderContainer = document.createElement('div');
            sliderContainer.className = 'range-slider mt-2';
            
            // 创建滑块
            const slider = document.createElement('input');
            slider.type = 'range';
            slider.min = min;
            slider.max = max;
            slider.step = step;
            slider.value = input.value;
            slider.className = 'form-range';
            
            // 当滑块值改变时，更新输入框的值
            slider.addEventListener('input', function() {
                input.value = this.value;
            });
            
            // 当输入框值改变时，更新滑块的值
            input.addEventListener('input', function() {
                slider.value = this.value;
            });
            
            // 将滑块添加到容器，然后将容器添加到输入框之后
            sliderContainer.appendChild(slider);
            input.parentNode.insertBefore(sliderContainer, input.nextSibling);
        });
    }
    
    // 为可折叠的元素添加动画效果
    const accordionButtons = document.querySelectorAll('.accordion-button');
    accordionButtons.forEach(button => {
        button.addEventListener('click', function() {
            // 添加过渡效果
            const collapse = document.querySelector(this.getAttribute('data-bs-target'));
            if (collapse) {
                collapse.style.transition = 'all 0.3s ease';
            }
        });
    });
    
    // 如果页面上有图表，确保它们在窗口大小调整时重新调整大小
    window.addEventListener('resize', function() {
        if (typeof Plotly !== 'undefined') {
            const charts = document.querySelectorAll('[id$="-chart"]');
            charts.forEach(chart => {
                Plotly.relayout(chart.id, {
                    width: chart.offsetWidth,
                    height: chart.offsetHeight
                });
            });
        }
    });
    
}); 